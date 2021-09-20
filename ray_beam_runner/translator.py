from typing import Mapping, Sequence

import ray.data

from apache_beam import (Create, Union, ParDo, Impulse, PTransform, GroupByKey,
                         WindowInto, Flatten, CoGroupByKey, io)
from apache_beam.internal.util import (insert_values_in_args)
from apache_beam.pipeline import AppliedPTransform, PipelineVisitor
from apache_beam.portability import common_urns
from apache_beam.pvalue import PBegin, TaggedOutput
from apache_beam.runners.common import MethodWrapper
from ray.data.block import Block, BlockMetadata, BlockAccessor

from ray_beam_runner.collection import CollectionMap
from ray_beam_runner.custom_actor_pool import CustomActorPool
from ray_beam_runner.overrides import (_Create, _Read, _Reshuffle)
from ray_beam_runner.side_input import (RaySideInput, RayMultiMapSideInput,
                                        RayIterableSideInput)
from ray_beam_runner.util import group_by_key
from apache_beam.transforms.window import WindowFn, TimestampedValue
from apache_beam.typehints import Optional
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import WindowedValue


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

        original_transform: io.Read = self.applied_ptransform.transform

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
        map_fn = transform.fn
        args = transform.args or []
        kwargs = transform.kwargs or {}

        # Todo: datasets are not iterable, so we fetch pandas dfs here
        # This is a ray get anti-pattern! This will not scale to
        # either many side inputs or to large dataset sizes. Fix this!
        def convert_pandas(ray_side_input: RaySideInput):
            df = ray.get(ray_side_input.ray_ds.to_pandas())[0]
            return ray_side_input.convert_df(df)

        args, kwargs = insert_values_in_args(args, kwargs, side_inputs)

        class RayDoFnWorker(object):
            def __init__(self):
                self._is_setup = False

                self._materialized_args = [
                    convert_pandas(arg)
                    if isinstance(arg, RaySideInput) else arg for arg in args
                ]
                self._materialized_kwargs = {
                    key: convert_pandas(val)
                    if isinstance(val, RaySideInput) else val
                    for key, val in kwargs.items()
                }

            def __del__(self):
                map_fn.teardown()

            def ready(self):
                return "ok"

            def process_batch(self, batch):
                map_fn.start_bundle()

                # map_fn.process may return multiple items
                ret = [
                    output_item for input_item in batch
                    for output_item in map_fn.process(
                        input_item, *self._materialized_args, **
                        self._materialized_kwargs)
                ]

                map_fn.finish_bundle()
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

        # The lambda fn is ignored as the RayDoFnWorker encapsulates the
        # actual logic in self.process_batch
        return ray_ds.map_batches(
            lambda batch: batch,
            compute=CustomActorPool(worker_cls=RayDoFnWorker))

        # Todo: fix value parsing and side input parsing

        method = MethodWrapper(map_fn, "process")

        def get_item_attributes(item):
            if isinstance(item, tuple):
                key, value = item
            else:
                key = None
                value = item

            if isinstance(value, WindowedValue):
                timestamp = value.timestamp
                window = value.windows[0]
                elem = value.value
            else:
                timestamp = None
                window = None
                elem = value

            return elem, key, timestamp, window

        def execute(item):
            elem, key, timestamp, window = get_item_attributes(item)

            kwargs = {}
            # if method.has_userstate_arguments:
            #   for kw, state_spec in method.state_args_to_replace.items():
            #     kwargs[kw] = user_state_context.get_state(
            #       state_spec, key, window)
            #   for kw, timer_spec in method.timer_args_to_replace.items():
            #     kwargs[kw] = user_state_context.get_timer(
            #       timer_spec, key, window, timestamp, pane_info)

            if method.timestamp_arg_name:
                kwargs[method.timestamp_arg_name] = Timestamp.of(timestamp)
            if method.window_arg_name:
                kwargs[method.window_arg_name] = window
            if method.key_arg_name:
                kwargs[method.key_arg_name] = key
            # if method.dynamic_timer_tag_arg_name:
            #   kwargs[method.dynamic_timer_tag_arg_name] = dynamic_timer_tag

            return method.method_value(elem, *new_args, **kwargs)

        return ray_ds.flat_map(execute)


class RayGroupByKey(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        assert isinstance(ray_ds, ray.data.Dataset)

        # Todo: Assert WindowedValue?

        df = ray.get(ray_ds.to_pandas())[0]
        grouped = group_by_key(df)

        return ray.data.from_items(list(grouped.items()))


class RayCoGroupByKey(RayDataTranslation):
    def apply(self,
              ray_ds: Union[None, ray.data.Dataset, Mapping[
                  str, ray.data.Dataset]] = None,
              side_inputs: Optional[Sequence[ray.data.Dataset]] = None):
        assert ray_ds is not None
        assert isinstance(ray_ds, ray.data.Dataset)

        raise RuntimeError("CoGroupByKey")

        return group_by_key(ray.get(ray_ds.to_pandas()[0]))


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
    WindowInto: RayNoop,  # RayWindowInto,
    GroupByKey: RayGroupByKey,
    CoGroupByKey: RayCoGroupByKey,
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

        ray_side_inputs = []
        for side_input in applied_ptransform.side_inputs:
            side_ds = self._collection_map.get(side_input.pvalue)
            input_data = side_input._side_input_data()
            if (input_data.access_pattern ==
                    common_urns.side_inputs.MULTIMAP.urn):
                wrapped_input = RayMultiMapSideInput(side_ds, input_data.view_fn)
            elif (input_data.access_pattern ==
                    common_urns.side_inputs.ITERABLE.urn):
                wrapped_input = RayIterableSideInput(side_ds, input_data.view_fn)
            else:
                raise RuntimeError(
                    f"Unknown side input access pattern: "
                    f"{input_data.access_pattern}")

            ray_side_inputs.append(wrapped_input)

        # print(
        #     "APPLYING", applied_ptransform.full_label,
        #     ray.get(ray_ds.to_pandas())
        #     if isinstance(ray_ds, ray.data.Dataset) else ray_ds)
        print("APPLYING", applied_ptransform.full_label, ray_ds)
        result = translation.apply(ray_ds, side_inputs=ray_side_inputs)
        # print(
        #     "RESULT",
        #     ray.get(result.to_pandas())
        #     if isinstance(result, ray.data.Dataset) else result)

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
