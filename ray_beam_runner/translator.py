from typing import Mapping, Sequence

import ray.data

from apache_beam import (Create, Union, ParDo, Impulse, PTransform, WindowInto,
                         Flatten, io, DoFn)
from apache_beam.pipeline import AppliedPTransform, PipelineVisitor
from apache_beam.pvalue import PBegin, TaggedOutput
from apache_beam.runners.common import DoFnInvoker, \
    DoFnSignature, DoFnContext, Receiver, _OutputProcessor
from apache_beam.transforms.sideinputs import SideInputMap
from ray.data.block import Block, BlockMetadata, BlockAccessor

from ray_beam_runner.collection import CollectionMap
from ray_beam_runner.custom_actor_pool import CustomActorPool
from ray_beam_runner.overrides import (_Create, _Read, _Reshuffle,
                                       _GroupByKeyOnly, _GroupAlsoByWindow)
from apache_beam.transforms.window import WindowFn, TimestampedValue, \
    GlobalWindow
from apache_beam.typehints import Optional
from apache_beam.utils.windowed_value import WindowedValue


def get_windowed_value(input_item, window_fn: WindowFn):
    if isinstance(input_item, TaggedOutput):
        input_item = input_item.value
    if isinstance(input_item, WindowedValue):
        windowed_value = input_item
    elif isinstance(input_item, TimestampedValue):
        assign_context = WindowFn.AssignContext(input_item.timestamp,
                                                input_item.value)
        windowed_value = WindowedValue(input_item.value, input_item.timestamp,
                                       window_fn.assign(assign_context))
    else:
        windowed_value = WindowedValue(input_item, 0, (GlobalWindow(), ))

    return windowed_value


class RayDataTranslation(object):
    def __init__(self,
                 applied_ptransform: AppliedPTransform,
                 parallelism: int = 1):
        self.applied_ptransform = applied_ptransform
        self.parallelism = parallelism

    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        raise NotImplementedError


class RayNoop(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        return ray_ds


class RayImpulse(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is None
        return ray.data.from_items([0], parallelism=self.parallelism)


class RayCreate(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is None

        original_transform: Create = self.applied_ptransform.transform

        items = original_transform.values

        # Todo: parallelism should be configurable
        # Setting this to < 1 leads to errors for assert_that checks
        return ray.data.from_items(items, parallelism=self.parallelism)


class RayRead(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is None

        original_transform: _Read = self.applied_ptransform.transform

        source = original_transform.source

        if isinstance(source, io.textio._TextSource):
            filename = source._pattern.value
            ray_ds = ray.data.read_text(filename, parallelism=self.parallelism)

            skip_lines = int(source._skip_header_lines)
            if skip_lines > 0:
                _, ray_ds = ray_ds.split_at_indices([skip_lines])

            return ray_ds

        raise NotImplementedError("Could not read from source:", source)


class RayReshuffle(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        return ray_ds.random_shuffle()


class RayParDo(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        assert isinstance(ray_ds, ray.data.Dataset)
        assert self.applied_ptransform.transform is not None
        assert isinstance(self.applied_ptransform.transform, ParDo)

        # Get original function and side inputs
        transform = self.applied_ptransform.transform
        label = transform.label
        map_fn = transform.fn
        args = transform.args or []
        kwargs = transform.kwargs or {}

        main_input = list(self.applied_ptransform.main_inputs.values())[0]
        window_fn = main_input.windowing.windowfn if hasattr(
            main_input, "windowing") else None

        class TaggingReceiver(Receiver):
            def __init__(self, tag, values):
                self.tag = tag
                self.values = values

            def receive(self, windowed_value):
                if self.tag:
                    output = TaggedOutput(self.tag, windowed_value)
                else:
                    output = windowed_value
                self.values.append(output)

        # We might want to have separate receivers at some point
        # For now, collect everything in one list and filter afterwards
        # class TaggedReceivers(dict):
        #     def __missing__(self, tag):
        #         self[tag] = receiver = SimpleReceiver()
        #         return receiver

        class OneReceiver(dict):
            def __init__(self, values):
                self.values = values

            def __missing__(self, key):
                if key not in self:
                    self[key] = TaggingReceiver(key, self.values)
                return self[key]

        class RayDoFnWorker(object):
            def __init__(self):
                self._is_setup = False

                self.context = DoFnContext(label, state=None)
                self.bundle_finalizer_param = DoFn.BundleFinalizerParam()
                do_fn_signature = DoFnSignature(map_fn)

                self.values = []

                self.tagged_receivers = OneReceiver(self.values)
                self.window_fn = window_fn

                output_processor = _OutputProcessor(
                    window_fn=self.window_fn,
                    main_receivers=self.tagged_receivers[None],
                    tagged_receivers=self.tagged_receivers,
                    per_element_output_counter=None,
                )

                self.do_fn_invoker = DoFnInvoker.create_invoker(
                    do_fn_signature,
                    output_processor,
                    self.context,
                    side_inputs,
                    args,
                    kwargs,
                    user_state_context=None,
                    bundle_finalizer_param=self.bundle_finalizer_param)

            def __del__(self):
                self.do_fn_invoker.invoke_teardown()

            def ready(self):
                return "ok"

            def process_batch(self, batch):
                if not self._is_setup:
                    self.do_fn_invoker.invoke_setup()
                    self._is_setup = True

                self.do_fn_invoker.invoke_start_bundle()

                # Clear return list
                self.values.clear()

                for input_item in batch:
                    windowed_value = get_windowed_value(
                        input_item, self.window_fn)
                    self.do_fn_invoker.invoke_process(windowed_value)

                self.do_fn_invoker.invoke_finish_bundle()

                # map_fn.process may return multiple items
                ret = list(self.values)
                return ret

            @ray.method(num_returns=2)
            def process_block(self, block: Block,
                              meta: BlockMetadata) -> (Block, BlockMetadata):
                if not self._is_setup:
                    map_fn.setup()
                    self._is_setup = True

                new_block = self.process_batch(block)
                accessor = BlockAccessor.for_block(new_block)
                new_metadata = BlockMetadata(
                    num_rows=accessor.num_rows(),
                    size_bytes=accessor.size_bytes(),
                    schema=accessor.schema(),
                    input_files=meta.input_files)
                return new_block, new_metadata

        def simple_batch_dofn(batch):
            context = DoFnContext(label, state=None)
            bundle_finalizer_param = DoFn.BundleFinalizerParam()
            do_fn_signature = DoFnSignature(map_fn)

            values = []

            tagged_receivers = OneReceiver(values)

            output_processor = _OutputProcessor(
                window_fn=window_fn,
                main_receivers=tagged_receivers[None],
                tagged_receivers=tagged_receivers,
                per_element_output_counter=None,
            )

            do_fn_invoker = DoFnInvoker.create_invoker(
                do_fn_signature,
                output_processor,
                context,
                side_inputs,
                args,
                kwargs,
                user_state_context=None,
                bundle_finalizer_param=bundle_finalizer_param)

            # Invoke setup just in case
            do_fn_invoker.invoke_setup()
            do_fn_invoker.invoke_start_bundle()

            for input_item in batch:
                windowed_value = get_windowed_value(input_item, window_fn)
                do_fn_invoker.invoke_process(windowed_value)

            do_fn_invoker.invoke_finish_bundle()
            # Invoke teardown just in case
            do_fn_invoker.invoke_teardown()

            # This has to happen last as we might receive results
            # in invoke_finish_bundle() or invoke_teardown()
            ret = list(values)

            return ret

        # Todo: implement
        dofn_has_no_setup_or_teardown = True

        if dofn_has_no_setup_or_teardown:
            return ray_ds.map_batches(simple_batch_dofn)

        # The lambda fn is ignored as the RayDoFnWorker encapsulates the
        # actual logic in self.process_batch
        return ray_ds.map_batches(
            lambda batch: batch,
            compute=CustomActorPool(worker_cls=RayDoFnWorker))


class RayGroupByKey(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        assert isinstance(ray_ds, ray.data.Dataset)

        # TODO(jjyao) Currently dataset doesn't handle
        # tuple groupby key so we wrap it in an object as a workaround.
        # This hack can be removed once dataset supports tuple groupby key.
        class KeyWrapper(object):
            def __init__(self, key):
                self.key = key

            def __lt__(self, other):
                return self.key < other.key

            def __eq__(self, other):
                return self.key == other.key

        def key(windowed_value):
            if not isinstance(windowed_value, WindowedValue):
                windowed_value = WindowedValue(windowed_value, 0,
                                           (GlobalWindow(), ))

            # Extract key from windowed value
            key, _ = windowed_value.value
            # We convert to strings here to support void keys
            return KeyWrapper(str(key) if key is None else key)

        def value(windowed_value):
            if not isinstance(windowed_value, WindowedValue):
                windowed_value = WindowedValue(windowed_value, 0,
                                           (GlobalWindow(), ))

            # Extract value from windowed value
            _, value = windowed_value.value
            return value

        return ray_ds.groupby(key).aggregate(ray.data.aggregate.AggregateFn(
            init=lambda k: [],
            accumulate=lambda a, r: a + [value(r)],
            merge=lambda a1, a2: a1 + a2,
        )).map(lambda r: (r[0].key, r[1]))


class RayWindowInto(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        window_fn = self.applied_ptransform.transform.windowing.windowfn

        def to_windowed_value(item):
            if isinstance(item, WindowedValue):
                return item

            if isinstance(item, TimestampedValue):
                return WindowedValue(
                    item.value, item.timestamp,
                    window_fn.assign(
                        WindowFn.AssignContext(
                            item.timestamp, element=item.value)))

            return item

        return ray_ds.map(to_windowed_value)


class RayFlatten(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        assert isinstance(ray_ds, Mapping)
        assert len(ray_ds) >= 1

        keys = sorted(ray_ds.keys())
        primary_key = keys.pop(0)

        primary_ds = ray_ds[primary_key]
        return primary_ds.union(*[ray_ds[key] for key in keys]).repartition(1)


translations = {
    _Create: RayCreate,  # Composite transform
    _Read: RayRead,
    Impulse: RayImpulse,
    _Reshuffle: RayReshuffle,
    ParDo: RayParDo,
    Flatten: RayFlatten,
    WindowInto: RayParDo,  # RayWindowInto,
    _GroupByKeyOnly: RayGroupByKey,
    _GroupAlsoByWindow: RayParDo,
    # CoGroupByKey: RayCoGroupByKey,
    PTransform: RayNoop  # Todo: How to handle generic ptransforms? Map?
}


class TranslationExecutor(PipelineVisitor):
    def __init__(self, collection_map: CollectionMap, parallelism: int = 1):
        self._collection_map = collection_map
        self._parallelism = parallelism

    def enter_composite_transform(self,
                                  transform_node: AppliedPTransform) -> None:
        pass

    def leave_composite_transform(self,
                                  transform_node: AppliedPTransform) -> None:
        pass

    def visit_transform(self, transform_node: AppliedPTransform) -> None:
        self.execute(transform_node)

    def get_translation(self, applied_ptransform: AppliedPTransform
                        ) -> Optional[RayDataTranslation]:
        # Sanity check
        type_ = type(applied_ptransform.transform)
        if type_ not in translations:
            return None

        translation_factory = translations[type_]
        translation = translation_factory(
            applied_ptransform, parallelism=self._parallelism)

        return translation

    def execute(self, applied_ptransform: AppliedPTransform) -> bool:
        translation = self.get_translation(applied_ptransform)

        if not translation:
            # Warn? Debug output?
            return False

        named_inputs = {}
        for name, element in applied_ptransform.named_inputs().items():
            if isinstance(element, PBegin):
                ray_ds = None
            else:
                ray_ds = self._collection_map.get(element)

            named_inputs[name] = ray_ds

        if len(named_inputs) == 0:
            ray_ds = None
        else:
            ray_ds = {}
            for name in applied_ptransform.main_inputs.keys():
                ray_ds[name] = named_inputs.pop(name)

            if len(ray_ds) == 1:
                ray_ds = list(ray_ds.values())[0]

        class RayDatasetAccessor(object):
            def __init__(self, ray_ds: ray.data.Dataset, window_fn: WindowFn):
                self.ray_ds = ray_ds
                self.window_fn = window_fn

            def __iter__(self):
                for row in self.ray_ds.iter_rows():
                    yield get_windowed_value(row, self.window_fn)

        side_inputs = []
        for side_input in applied_ptransform.side_inputs:
            side_ds = self._collection_map.get(side_input.pvalue)
            side_inputs.append(
                SideInputMap(
                    type(side_input), side_input._view_options(),
                    RayDatasetAccessor(side_ds,
                                       side_input._window_mapping_fn)))

        def _visualize(ray_ds_dict):
            for name, ray_ds in ray_ds_dict.items():
                if not ray_ds:
                    out = ray_ds
                elif not isinstance(ray_ds, ray.data.Dataset):
                    out = ray_ds
                else:
                    out = ray.get(ray_ds.to_numpy_refs())
                print(("DATA", name, out))
                continue

        def _visualize_all(ray_ds):
            if isinstance(ray_ds, ray.data.Dataset) or not ray_ds:
                _visualize({"_main": ray_ds})
            elif isinstance(ray_ds, list):
                _visualize((dict(enumerate(ray_ds))))
            else:
                _visualize(ray_ds)

        print("-" * 80)
        print("APPLYING", applied_ptransform.full_label)
        print("-" * 80)
        print("MAIN INPUT")
        _visualize_all(ray_ds)
        print("SIDE INPUTS")
        _visualize_all([list(si._iterable) for si in side_inputs])
        print("." * 40)
        result = translation.apply(ray_ds, side_inputs=side_inputs)
        print("." * 40)
        print("RESULT", applied_ptransform.full_label)
        _visualize_all(result)
        print("-" * 80)

        for name, element in applied_ptransform.named_outputs().items():
            if isinstance(result, dict):
                out = result.get(name)
            else:
                out = result

            if out:
                if name != "None":
                    # Side output
                    out = out.filter(lambda x: isinstance(x, TaggedOutput) and
                                     x.tag == name)
                    out = out.map(lambda x: x.value)
                else:
                    # Main output
                    out = out.filter(lambda x: not isinstance(x, TaggedOutput))

            self._collection_map.set(element, out)

        return True
