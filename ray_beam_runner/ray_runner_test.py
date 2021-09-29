# flake8: noqa
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# pytype: skip-file

import collections
import gc
import logging
import os
import random
import re
import shutil
import tempfile
import threading
import time
import traceback
import typing
import unittest
from typing import Any
from typing import Tuple

import hamcrest  # pylint: disable=ungrouped-imports
from apache_beam.runners.portability.fn_api_runner.fn_runner_test import \
    ExpandStringsProvider, CustomMergingWindowFn
from hamcrest.core.matcher import Matcher

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.metrics import monitoring_infos
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.value_provider import RuntimeValueProvider
from apache_beam.portability import python_urns
from ray_beam_runner import ray_runner
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.worker import data_plane
from apache_beam.runners.worker import statesampler
from apache_beam.testing.synthetic_pipeline import SyntheticSDFAsSource
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.utils import timestamp

if statesampler.FAST_SAMPLER:
    DEFAULT_SAMPLING_PERIOD_MS = statesampler.DEFAULT_SAMPLING_PERIOD_MS
else:
    DEFAULT_SAMPLING_PERIOD_MS = 0

_LOGGER = logging.getLogger(__name__)


def _matcher_or_equal_to(value_or_matcher):
    """Pass-thru for matchers, and wraps value inputs in an equal_to matcher."""
    if value_or_matcher is None:
        return None
    if isinstance(value_or_matcher, Matcher):
        return value_or_matcher
    return hamcrest.equal_to(value_or_matcher)


def has_urn_and_labels(mi, urn, labels):
    """Returns true if it the monitoring_info contains the labels and urn."""

    def contains_labels(mi, labels):
        # Check all the labels and their values exist in the monitoring_info
        return all(item in mi.labels.items() for item in labels.items())

    return contains_labels(mi, labels) and mi.urn == urn


class RayRunnerTest(unittest.TestCase):
    def setUp(self) -> None:
        import ray
        if not ray.is_initialized():
            ray.init(local_mode=True)

    def create_pipeline(self, is_drain=False):
        return beam.Pipeline(
            runner=ray_runner.RayRunner(),
            options=PipelineOptions(["--parallelism=1"]))

    def test_assert_that(self):
        # TODO: figure out a way for fn_api_runner to parse and raise the
        # underlying exception.
        return  # Todo: Enable
        with self.assertRaisesRegex(Exception, 'Failed assert'):
            with self.create_pipeline() as p:
                assert_that(p | beam.Create(['a', 'b']), equal_to(['a']))

    def test_create(self):
        with self.create_pipeline() as p:
            assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

    def test_pardo(self):
        with self.create_pipeline() as p:
            res = (p
                   | beam.Create(['a', 'bc'])
                   | beam.Map(lambda e: e * 2)
                   | beam.Map(lambda e: e + 'x'))
            assert_that(res, equal_to(['aax', 'bcbcx']))

    def test_pardo_side_outputs(self):
        def tee(elem, *tags):
            for tag in tags:
                if tag in elem:
                    yield beam.pvalue.TaggedOutput(tag, elem)

        with self.create_pipeline() as p:
            xy = (p
                  | 'Create' >> beam.Create(['x', 'y', 'xy'])
                  | beam.FlatMap(tee, 'x', 'y').with_outputs())
            assert_that(xy.x, equal_to(['x', 'xy']), label='x')
            assert_that(xy.y, equal_to(['y', 'xy']), label='y')

    def test_pardo_side_and_main_outputs(self):
        def even_odd(elem):
            yield elem
            yield beam.pvalue.TaggedOutput('odd' if elem % 2 else 'even', elem)

        with self.create_pipeline() as p:
            ints = p | beam.Create([1, 2, 3])
            named = ints | 'named' >> beam.FlatMap(even_odd).with_outputs(
                'even', 'odd', main='all')
            assert_that(named.all, equal_to([1, 2, 3]), label='named.all')
            assert_that(named.even, equal_to([2]), label='named.even')
            assert_that(named.odd, equal_to([1, 3]), label='named.odd')

            unnamed = ints | 'unnamed' >> beam.FlatMap(even_odd).with_outputs()
            unnamed[None] | beam.Map(id)  # pylint: disable=expression-not-assigned
            assert_that(
                unnamed[None], equal_to([1, 2, 3]), label='unnamed.all')
            assert_that(unnamed.even, equal_to([2]), label='unnamed.even')
            assert_that(unnamed.odd, equal_to([1, 3]), label='unnamed.odd')

    def test_pardo_side_inputs(self):
        def cross_product(elem, sides):
            for side in sides:
                yield elem, side

        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create(['a', 'b', 'c'])
            side = p | 'side' >> beam.Create(['x', 'y'])
            assert_that(
                main | beam.FlatMap(cross_product, beam.pvalue.AsList(side)),
                equal_to([('a', 'x'), ('b', 'x'), ('c', 'x'), ('a', 'y'),
                          ('b', 'y'), ('c', 'y')]))

    def test_pardo_windowed_side_inputs(self):
        with self.create_pipeline() as p:
            # Now with some windowing.
            pcoll = p | beam.Create(list(
                range(10))) | beam.Map(lambda t: window.TimestampedValue(t, t))
            # Intentionally choosing non-aligned windows to highlight the transition.
            main = pcoll | 'WindowMain' >> beam.WindowInto(
                window.FixedWindows(5))
            side = pcoll | 'WindowSide' >> beam.WindowInto(
                window.FixedWindows(7))
            res = main | beam.Map(lambda x, s: (x, sorted(s)),
                                  beam.pvalue.AsList(side))
            assert_that(
                res,
                equal_to([
                    # The window [0, 5) maps to the window [0, 7).
                    (0, list(range(7))),
                    (1, list(range(7))),
                    (2, list(range(7))),
                    (3, list(range(7))),
                    (4, list(range(7))),
                    # The window [5, 10) maps to the window [7, 14).
                    (5, list(range(7, 10))),
                    (6, list(range(7, 10))),
                    (7, list(range(7, 10))),
                    (8, list(range(7, 10))),
                    (9, list(range(7, 10)))
                ]),
                label='windowed')

    def test_flattened_side_input(self, with_transcoding=True):
        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create([None])
            side1 = p | 'side1' >> beam.Create([('a', 1)])
            side2 = p | 'side2' >> beam.Create([('b', 2)])
            if with_transcoding:
                # Also test non-matching coder types (transcoding required)
                third_element = [('another_type')]
            else:
                third_element = [('b', 3)]
            side3 = p | 'side3' >> beam.Create(third_element)
            side = (side1, side2) | beam.Flatten()
            assert_that(
                main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
                equal_to([(None, {
                    'a': 1,
                    'b': 2
                })]),
                label='CheckFlattenAsSideInput')
            assert_that(
                (side, side3) | 'FlattenAfter' >> beam.Flatten(),
                equal_to([('a', 1), ('b', 2)] + third_element),
                label='CheckFlattenOfSideInput')

    def test_gbk_side_input(self):
        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create([None])
            side = p | 'side' >> beam.Create([('a', 1)]) | beam.GroupByKey()
            assert_that(
                main | beam.Map(lambda a, b: (a, b), beam.pvalue.AsDict(side)),
                equal_to([(None, {
                    'a': [1]
                })]))

    def test_multimap_side_input(self):
        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create(['a', 'b'])
            side = p | 'side' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
            assert_that(
                main | beam.Map(lambda k, d: (k, sorted(d[k])),
                                beam.pvalue.AsMultiMap(side)),
                equal_to([('a', [1, 3]), ('b', [2])]))

    def test_multimap_multiside_input(self):
        # A test where two transforms in the same stage consume the same PCollection
        # twice as side input.
        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create(['a', 'b'])
            side = p | 'side' >> beam.Create([('a', 1), ('b', 2), ('a', 3)])
            assert_that(
                main | 'first map' >> beam.Map(
                    lambda k, d, l:
                    (k, sorted(d[k]), sorted([e[1] for e in l])),
                    beam.pvalue.AsMultiMap(side), beam.pvalue.AsList(side))
                | 'second map' >> beam.Map(
                    lambda k, d, l:
                    (k[0], sorted(d[k[0]]), sorted([e[1] for e in l])),
                    beam.pvalue.AsMultiMap(side), beam.pvalue.AsList(side)),
                equal_to([('a', [1, 3], [1, 2, 3]), ('b', [2], [1, 2, 3])]))

    def test_multimap_side_input_type_coercion(self):
        with self.create_pipeline() as p:
            main = p | 'main' >> beam.Create(['a', 'b'])
            # The type of this side-input is forced to Any (overriding type
            # inference). Without type coercion to Tuple[Any, Any], the usage of this
            # side-input in AsMultiMap() below should fail.
            side = (p | 'side' >> beam.Create(
                [('a', 1), ('b', 2), ('a', 3)]).with_output_types(typing.Any))
            assert_that(
                main | beam.Map(lambda k, d: (k, sorted(d[k])),
                                beam.pvalue.AsMultiMap(side)),
                equal_to([('a', [1, 3]), ('b', [2])]))

    def test_pardo_unfusable_side_inputs(self):
        def cross_product(elem, sides):
            for side in sides:
                yield elem, side

        with self.create_pipeline() as p:
            pcoll = p | beam.Create(['a', 'b'])
            assert_that(
                pcoll | beam.FlatMap(cross_product, beam.pvalue.AsList(pcoll)),
                equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

        with self.create_pipeline() as p:
            pcoll = p | beam.Create(['a', 'b'])
            derived = ((pcoll, ) | beam.Flatten()
                       | beam.Map(lambda x: (x, x))
                       | beam.GroupByKey()
                       | 'Unkey' >> beam.Map(lambda kv: kv[0]))
            assert_that(
                pcoll
                | beam.FlatMap(cross_product, beam.pvalue.AsList(derived)),
                equal_to([('a', 'a'), ('a', 'b'), ('b', 'a'), ('b', 'b')]))

    def test_pardo_state_only(self):
        index_state_spec = userstate.CombiningValueStateSpec('index', sum)
        value_and_index_state_spec = userstate.ReadModifyWriteStateSpec(
            'value:index', StrUtf8Coder())

        # TODO(ccy): State isn't detected with Map/FlatMap.
        class AddIndex(beam.DoFn):
            def process(self,
                        kv,
                        index=beam.DoFn.StateParam(index_state_spec),
                        value_and_index=beam.DoFn.StateParam(
                            value_and_index_state_spec)):
                k, v = kv
                index.add(1)
                value_and_index.write('%s:%s' % (v, index.read()))
                yield k, v, index.read(), value_and_index.read()

        inputs = [('A', 'a')] * 2 + [('B', 'b')] * 3
        expected = [('A', 'a', 1, 'a:1'), ('A', 'a', 2, 'a:2'),
                    ('B', 'b', 1, 'b:1'), ('B', 'b', 2, 'b:2'), ('B', 'b', 3,
                                                                 'b:3')]

        with self.create_pipeline() as p:
            assert_that(p | beam.Create(inputs) | beam.ParDo(AddIndex()),
                        equal_to(expected))

    @unittest.skip('TestStream not yet supported')
    def test_teststream_pardo_timers(self):
        timer_spec = userstate.TimerSpec('timer',
                                         userstate.TimeDomain.WATERMARK)

        class TimerDoFn(beam.DoFn):
            def process(self, element, timer=beam.DoFn.TimerParam(timer_spec)):
                unused_key, ts = element
                timer.set(ts)
                timer.set(2 * ts)

            @userstate.on_timer(timer_spec)
            def process_timer(self):
                yield 'fired'

        ts = (
            TestStream().add_elements([('k1', 10)])  # Set timer for 20
            .advance_watermark_to(100).add_elements(
                [('k2', 100)])  # Set timer for 200
            .advance_watermark_to(1000))

        with self.create_pipeline() as p:
            _ = (p
                 | ts
                 | beam.ParDo(TimerDoFn())
                 | beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

            #expected = [('fired', ts) for ts in (20, 200)]
            #assert_that(actual, equal_to(expected))

    def test_pardo_timers(self):
        timer_spec = userstate.TimerSpec('timer',
                                         userstate.TimeDomain.WATERMARK)
        state_spec = userstate.CombiningValueStateSpec('num_called', sum)

        class TimerDoFn(beam.DoFn):
            def process(self, element, timer=beam.DoFn.TimerParam(timer_spec)):
                unused_key, ts = element
                timer.set(ts)
                timer.set(2 * ts)

            @userstate.on_timer(timer_spec)
            def process_timer(self,
                              ts=beam.DoFn.TimestampParam,
                              timer=beam.DoFn.TimerParam(timer_spec),
                              state=beam.DoFn.StateParam(state_spec)):
                if state.read() == 0:
                    state.add(1)
                    timer.set(timestamp.Timestamp(micros=2 * ts.micros))
                yield 'fired'

        with self.create_pipeline() as p:
            actual = (p
                      | beam.Create([('k1', 10), ('k2', 100)])
                      | beam.ParDo(TimerDoFn())
                      |
                      beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

            expected = [('fired', ts) for ts in (20, 200, 40, 400)]
            assert_that(actual, equal_to(expected))

    def test_pardo_timers_clear(self):
        timer_spec = userstate.TimerSpec('timer',
                                         userstate.TimeDomain.WATERMARK)
        clear_timer_spec = userstate.TimerSpec('clear_timer',
                                               userstate.TimeDomain.WATERMARK)

        class TimerDoFn(beam.DoFn):
            def process(self,
                        element,
                        timer=beam.DoFn.TimerParam(timer_spec),
                        clear_timer=beam.DoFn.TimerParam(clear_timer_spec)):
                unused_key, ts = element
                timer.set(ts)
                timer.set(2 * ts)
                clear_timer.set(ts)
                clear_timer.clear()

            @userstate.on_timer(timer_spec)
            def process_timer(self):
                yield 'fired'

            @userstate.on_timer(clear_timer_spec)
            def process_clear_timer(self):
                yield 'should not fire'

        with self.create_pipeline() as p:
            actual = (p
                      | beam.Create([('k1', 10), ('k2', 100)])
                      | beam.ParDo(TimerDoFn())
                      |
                      beam.Map(lambda x, ts=beam.DoFn.TimestampParam: (x, ts)))

            expected = [('fired', ts) for ts in (20, 200)]
            assert_that(actual, equal_to(expected))

    def test_pardo_state_timers(self):
        self._run_pardo_state_timers(windowed=False)

    def test_pardo_state_timers_non_standard_coder(self):
        self._run_pardo_state_timers(windowed=False, key_type=Any)

    def test_windowed_pardo_state_timers(self):
        self._run_pardo_state_timers(windowed=True)

    def _run_pardo_state_timers(self, windowed, key_type=None):
        """
    :param windowed: If True, uses an interval window, otherwise a global window
    :param key_type: Allows to override the inferred key type. This is useful to
    test the use of non-standard coders, e.g. Python's FastPrimitivesCoder.
    """
        state_spec = userstate.BagStateSpec('state',
                                            beam.coders.StrUtf8Coder())
        timer_spec = userstate.TimerSpec('timer',
                                         userstate.TimeDomain.WATERMARK)
        elements = list('abcdefgh')
        key = 'key'
        buffer_size = 3

        class BufferDoFn(beam.DoFn):
            def process(self,
                        kv,
                        ts=beam.DoFn.TimestampParam,
                        timer=beam.DoFn.TimerParam(timer_spec),
                        state=beam.DoFn.StateParam(state_spec)):
                _, element = kv
                state.add(element)
                buffer = state.read()
                # For real use, we'd keep track of this size separately.
                if len(list(buffer)) >= 3:
                    state.clear()
                    yield buffer
                else:
                    timer.set(ts + 1)

            @userstate.on_timer(timer_spec)
            def process_timer(self, state=beam.DoFn.StateParam(state_spec)):
                buffer = state.read()
                state.clear()
                yield buffer

        def is_buffered_correctly(actual):
            # Pickling self in the closure for asserts gives errors (only on jenkins).
            self = FnApiRunnerTest('__init__')
            # Acutal should be a grouping of the inputs into batches of size
            # at most buffer_size, but the actual batching is nondeterministic
            # based on ordering and trigger firing timing.
            self.assertEqual(
                sorted(sum((list(b) for b in actual), [])), elements)
            self.assertEqual(
                max(len(list(buffer)) for buffer in actual), buffer_size)
            if windowed:
                # Elements were assigned to windows based on their parity.
                # Assert that each grouping consists of elements belonging to the
                # same window to ensure states and timers were properly partitioned.
                for b in actual:
                    parity = set(ord(e) % 2 for e in b)
                    self.assertEqual(1, len(parity), b)

        with self.create_pipeline() as p:
            actual = (
                p
                | beam.Create(elements)
                # Send even and odd elements to different windows.
                | beam.Map(lambda e: window.TimestampedValue(e,
                                                             ord(e) % 2))
                | beam.WindowInto(
                    window.FixedWindows(1)
                    if windowed else window.GlobalWindows())
                | beam.Map(lambda x: (key, x)).with_output_types(
                    Tuple[key_type if key_type else type(key), Any])
                | beam.ParDo(BufferDoFn()))

            assert_that(actual, is_buffered_correctly)

    def test_pardo_dynamic_timer(self):
        class DynamicTimerDoFn(beam.DoFn):
            dynamic_timer_spec = userstate.TimerSpec(
                'dynamic_timer', userstate.TimeDomain.WATERMARK)

            def process(
                    self,
                    element,
                    dynamic_timer=beam.DoFn.TimerParam(dynamic_timer_spec)):
                dynamic_timer.set(element[1], dynamic_timer_tag=element[0])

            @userstate.on_timer(dynamic_timer_spec)
            def dynamic_timer_callback(self,
                                       tag=beam.DoFn.DynamicTimerTagParam,
                                       timestamp=beam.DoFn.TimestampParam):
                yield (tag, timestamp)

        with self.create_pipeline() as p:
            actual = (p
                      | beam.Create([('key1', 10), ('key2', 20), ('key3', 30)])
                      | beam.ParDo(DynamicTimerDoFn()))
            assert_that(actual,
                        equal_to([('key1', 10), ('key2', 20), ('key3', 30)]))

    def test_sdf(self):
        class ExpandingStringsDoFn(beam.DoFn):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            ExpandStringsProvider())):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield element[cur]
                    cur += 1

        with self.create_pipeline() as p:
            data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
            actual = (p | beam.Create(data)
                      | beam.ParDo(ExpandingStringsDoFn()))
            assert_that(actual, equal_to(list(''.join(data))))

    def test_sdf_with_dofn_as_restriction_provider(self):
        class ExpandingStringsDoFn(beam.DoFn, ExpandStringsProvider):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam()):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield element[cur]
                    cur += 1

        with self.create_pipeline() as p:
            data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
            actual = (p | beam.Create(data)
                      | beam.ParDo(ExpandingStringsDoFn()))
            assert_that(actual, equal_to(list(''.join(data))))

    def test_sdf_with_check_done_failed(self):
        class ExpandingStringsDoFn(beam.DoFn):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            ExpandStringsProvider())):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield element[cur]
                    cur += 1
                    return

        with self.assertRaises(Exception):
            with self.create_pipeline() as p:
                data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
                _ = (p | beam.Create(data)
                     | beam.ParDo(ExpandingStringsDoFn()))

    def test_sdf_with_watermark_tracking(self):
        class ExpandingStringsDoFn(beam.DoFn):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            ExpandStringsProvider()),
                        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
                            ManualWatermarkEstimator.default_provider())):
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    watermark_estimator.set_watermark(timestamp.Timestamp(cur))
                    assert (watermark_estimator.current_watermark() ==
                            timestamp.Timestamp(cur))
                    yield element[cur]
                    if cur % 2 == 1:
                        restriction_tracker.defer_remainder(
                            timestamp.Duration(micros=5))
                        return
                    cur += 1

        with self.create_pipeline() as p:
            data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
            actual = (p | beam.Create(data)
                      | beam.ParDo(ExpandingStringsDoFn()))
            assert_that(actual, equal_to(list(''.join(data))))

    def test_sdf_with_dofn_as_watermark_estimator(self):
        class ExpandingStringsDoFn(beam.DoFn, beam.WatermarkEstimatorProvider):
            def initial_estimator_state(self, element, restriction):
                return None

            def create_watermark_estimator(self, state):
                return beam.io.watermark_estimators.ManualWatermarkEstimator(
                    state)

            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            ExpandStringsProvider()),
                        watermark_estimator=beam.DoFn.WatermarkEstimatorParam(
                            ManualWatermarkEstimator.default_provider())):
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    watermark_estimator.set_watermark(timestamp.Timestamp(cur))
                    assert (watermark_estimator.current_watermark() ==
                            timestamp.Timestamp(cur))
                    yield element[cur]
                    if cur % 2 == 1:
                        restriction_tracker.defer_remainder(
                            timestamp.Duration(micros=5))
                        return
                    cur += 1

        with self.create_pipeline() as p:
            data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
            actual = (p | beam.Create(data)
                      | beam.ParDo(ExpandingStringsDoFn()))
            assert_that(actual, equal_to(list(''.join(data))))

    def run_sdf_initiated_checkpointing(self, is_drain=False):
        counter = beam.metrics.Metrics.counter('ns', 'my_counter')

        class ExpandStringsDoFn(beam.DoFn):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            ExpandStringsProvider())):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    counter.inc()
                    yield element[cur]
                    if cur % 2 == 1:
                        restriction_tracker.defer_remainder()
                        return
                    cur += 1

        with self.create_pipeline(is_drain=is_drain) as p:
            data = ['abc', 'defghijklmno', 'pqrstuv', 'wxyz']
            actual = (p | beam.Create(data) | beam.ParDo(ExpandStringsDoFn()))

            assert_that(actual, equal_to(list(''.join(data))))

        if isinstance(p.runner, fn_api_runner.FnApiRunner):
            res = p.runner._latest_run_result
            counters = res.metrics().query(
                beam.metrics.MetricsFilter())['counters']
            self.assertEqual(1, len(counters))
            self.assertEqual(counters[0].committed, len(''.join(data)))

    def test_sdf_with_sdf_initiated_checkpointing(self):
        self.run_sdf_initiated_checkpointing(is_drain=False)

    def test_draining_sdf_with_sdf_initiated_checkpointing(self):
        self.run_sdf_initiated_checkpointing(is_drain=True)

    def test_sdf_default_truncate_when_bounded(self):
        class SimleSDF(beam.DoFn):
            def process(
                    self,
                    element,
                    restriction_tracker=beam.DoFn.RestrictionParam(
                        OffsetRangeProvider(use_bounded_offset_range=True))):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield cur
                    cur += 1

        with self.create_pipeline(is_drain=True) as p:
            actual = (p | beam.Create([10]) | beam.ParDo(SimleSDF()))
            assert_that(actual, equal_to(range(10)))

    def test_sdf_default_truncate_when_unbounded(self):
        class SimleSDF(beam.DoFn):
            def process(
                    self,
                    element,
                    restriction_tracker=beam.DoFn.RestrictionParam(
                        OffsetRangeProvider(use_bounded_offset_range=False))):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield cur
                    cur += 1

        with self.create_pipeline(is_drain=True) as p:
            actual = (p | beam.Create([10]) | beam.ParDo(SimleSDF()))
            assert_that(actual, equal_to([]))

    def test_sdf_with_truncate(self):
        class SimleSDF(beam.DoFn):
            def process(self,
                        element,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            OffsetRangeProviderWithTruncate())):
                assert isinstance(restriction_tracker, RestrictionTrackerView)
                cur = restriction_tracker.current_restriction().start
                while restriction_tracker.try_claim(cur):
                    yield cur
                    cur += 1

        with self.create_pipeline(is_drain=True) as p:
            actual = (p | beam.Create([10]) | beam.ParDo(SimleSDF()))
            assert_that(actual, equal_to(range(5)))

    def test_group_by_key(self):
        with self.create_pipeline() as p:
            res = (p
                   | beam.Create([('a', 1), ('a', 2), ('b', 3)])
                   | beam.GroupByKey()
                   | beam.Map(lambda k_vs: (k_vs[0], sorted(k_vs[1]))))
            assert_that(res, equal_to([('a', [1, 2]), ('b', [3])]))

    # Runners may special case the Reshuffle transform urn.
    def test_reshuffle(self):
        with self.create_pipeline() as p:
            assert_that(p | beam.Create([1, 2, 3]) | beam.Reshuffle(),
                        equal_to([1, 2, 3]))

    def test_flatten(self, with_transcoding=True):
        with self.create_pipeline() as p:
            if with_transcoding:
                # Additional element which does not match with the first type
                additional = [ord('d')]
            else:
                additional = ['d']
            res = (p | 'a' >> beam.Create(['a']),
                   p | 'bc' >> beam.Create(['b', 'c']),
                   p | 'd' >> beam.Create(additional)) | beam.Flatten()
            assert_that(res, equal_to(['a', 'b', 'c'] + additional))

    def test_flatten_same_pcollections(self, with_transcoding=True):
        with self.create_pipeline() as p:
            pc = p | beam.Create(['a', 'b'])
            assert_that((pc, pc, pc) | beam.Flatten(),
                        equal_to(['a', 'b'] * 3))

    def test_combine_per_key(self):
        with self.create_pipeline() as p:
            res = (p
                   | beam.Create([('a', 1), ('a', 2), ('b', 3)])
                   | beam.CombinePerKey(beam.combiners.MeanCombineFn()))
            assert_that(res, equal_to([('a', 1.5), ('b', 3.0)]))

    def test_read(self):
        # Can't use NamedTemporaryFile as a context
        # due to https://bugs.python.org/issue14243
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            temp_file.write(b'a\nb\nc')
            temp_file.close()
            with self.create_pipeline() as p:
                assert_that(p | beam.io.ReadFromText(temp_file.name),
                            equal_to(['a', 'b', 'c']))
        finally:
            os.unlink(temp_file.name)

    def test_windowing(self):
        with self.create_pipeline() as p:
            res = (p
                   | beam.Create([1, 2, 100, 101, 102])
                   | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
                   | beam.WindowInto(beam.transforms.window.Sessions(10))
                   | beam.GroupByKey()
                   | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
            assert_that(res, equal_to([('k', [1, 2]), ('k', [100, 101, 102])]))

    def test_custom_merging_window(self):
        with self.create_pipeline() as p:
            res = (p
                   | beam.Create([1, 2, 100, 101, 102])
                   | beam.Map(lambda t: window.TimestampedValue(('k', t), t))
                   | beam.WindowInto(CustomMergingWindowFn())
                   | beam.GroupByKey()
                   | beam.Map(lambda k_vs1: (k_vs1[0], sorted(k_vs1[1]))))
            assert_that(
                res, equal_to([('k', [1]), ('k', [101]), ('k', [2, 100,
                                                                102])]))
        gc.collect()
        from apache_beam.runners.portability.fn_api_runner.execution import GenericMergingWindowFn
        self.assertEqual(GenericMergingWindowFn._HANDLES, {})

    @unittest.skip('BEAM-9119: test is flaky')
    def test_large_elements(self):
        with self.create_pipeline() as p:
            big = (p
                   | beam.Create(['a', 'a', 'b'])
                   | beam.Map(lambda x: (x, x * data_plane.
                                         _DEFAULT_SIZE_FLUSH_THRESHOLD)))

            side_input_res = (
                big
                | beam.Map(lambda x, side: (x[0], side.count(x[0])),
                           beam.pvalue.AsList(big | beam.Map(lambda x: x[0]))))
            assert_that(
                side_input_res,
                equal_to([('a', 2), ('a', 2), ('b', 1)]),
                label='side')

            gbk_res = (big | beam.GroupByKey() | beam.Map(lambda x: x[0]))
            assert_that(gbk_res, equal_to(['a', 'b']), label='gbk')

    def test_error_message_includes_stage(self):
        with self.assertRaises(BaseException) as e_cm:
            with self.create_pipeline() as p:

                def raise_error(x):
                    raise RuntimeError('x')

                # pylint: disable=expression-not-assigned
                (p
                 | beam.Create(['a', 'b'])
                 | 'StageA' >> beam.Map(lambda x: x)
                 | 'StageB' >> beam.Map(lambda x: x)
                 | 'StageC' >> beam.Map(raise_error)
                 | 'StageD' >> beam.Map(lambda x: x))
        message = e_cm.exception.args[0]
        self.assertIn('StageC', message)
        self.assertNotIn('StageB', message)

    def test_error_traceback_includes_user_code(self):
        def first(x):
            return second(x)

        def second(x):
            return third(x)

        def third(x):
            raise ValueError('x')

        try:
            with self.create_pipeline() as p:
                p | beam.Create([0]) | beam.Map(first)  # pylint: disable=expression-not-assigned
        except Exception:  # pylint: disable=broad-except
            message = traceback.format_exc()
        else:
            raise AssertionError('expected exception not raised')

        self.assertIn('first', message)
        self.assertIn('second', message)
        self.assertIn('third', message)

    def test_no_subtransform_composite(self):
        class First(beam.PTransform):
            def expand(self, pcolls):
                return pcolls[0]

        with self.create_pipeline() as p:
            pcoll_a = p | 'a' >> beam.Create(['a'])
            pcoll_b = p | 'b' >> beam.Create(['b'])
            assert_that((pcoll_a, pcoll_b) | First(), equal_to(['a']))

    def test_metrics(self, check_gauge=True):
        p = self.create_pipeline()

        counter = beam.metrics.Metrics.counter('ns', 'counter')
        distribution = beam.metrics.Metrics.distribution('ns', 'distribution')
        gauge = beam.metrics.Metrics.gauge('ns', 'gauge')

        pcoll = p | beam.Create(['a', 'zzz'])
        # pylint: disable=expression-not-assigned
        pcoll | 'count1' >> beam.FlatMap(lambda x: counter.inc())
        pcoll | 'count2' >> beam.FlatMap(lambda x: counter.inc(len(x)))
        pcoll | 'dist' >> beam.FlatMap(lambda x: distribution.update(len(x)))
        pcoll | 'gauge' >> beam.FlatMap(lambda x: gauge.set(3))

        res = p.run()
        res.wait_until_finish()

        t1, t2 = res.metrics().query(beam.metrics.MetricsFilter()
                                     .with_name('counter'))['counters']
        self.assertEqual(t1.committed + t2.committed, 6)

        dist, = res.metrics().query(
            beam.metrics.MetricsFilter()
            .with_name('distribution'))['distributions']
        self.assertEqual(dist.committed.data,
                         beam.metrics.cells.DistributionData(4, 2, 1, 3))
        self.assertEqual(dist.committed.mean, 2.0)

        if check_gauge:
            gaug, = res.metrics().query(beam.metrics.MetricsFilter()
                                        .with_name('gauge'))['gauges']
            self.assertEqual(gaug.committed.value, 3)

    def test_callbacks_with_exception(self):
        elements_list = ['1', '2']

        def raise_expetion():
            raise Exception('raise exception when calling callback')

        class FinalizebleDoFnWithException(beam.DoFn):
            def process(self,
                        element,
                        bundle_finalizer=beam.DoFn.BundleFinalizerParam):
                bundle_finalizer.register(raise_expetion)
                yield element

        with self.create_pipeline() as p:
            res = (p
                   | beam.Create(elements_list)
                   | beam.ParDo(FinalizebleDoFnWithException()))
            assert_that(res, equal_to(['1', '2']))

    def test_register_finalizations(self):
        event_recorder = EventRecorder(tempfile.gettempdir())

        class FinalizableSplittableDoFn(beam.DoFn):
            def process(self,
                        element,
                        bundle_finalizer=beam.DoFn.BundleFinalizerParam,
                        restriction_tracker=beam.DoFn.RestrictionParam(
                            OffsetRangeProvider(
                                use_bounded_offset_range=True,
                                checkpoint_only=True))):
                # We use SDF to enforce finalization call happens by using
                # self-initiated checkpoint.
                if 'finalized' in event_recorder.events():
                    restriction_tracker.try_claim(
                        restriction_tracker.current_restriction().start)
                    yield element
                    restriction_tracker.try_claim(element)
                    return
                if restriction_tracker.try_claim(
                        restriction_tracker.current_restriction().start):
                    bundle_finalizer.register(lambda: event_recorder.record(
                        'finalized'))
                    # We sleep here instead of setting a resume time since the resume time
                    # doesn't need to be honored.
                    time.sleep(1)
                    restriction_tracker.defer_remainder()

        with self.create_pipeline() as p:
            max_retries = 100
            res = (p
                   | beam.Create([max_retries])
                   | beam.ParDo(FinalizableSplittableDoFn()))
            assert_that(res, equal_to([max_retries]))

        event_recorder.cleanup()

    def test_sdf_synthetic_source(self):
        common_attrs = {
            'key_size': 1,
            'value_size': 1,
            'initial_splitting_num_bundles': 2,
            'initial_splitting_desired_bundle_size': 2,
            'sleep_per_input_record_sec': 0,
            'initial_splitting': 'const'
        }
        num_source_description = 5
        min_num_record = 10
        max_num_record = 20

        # pylint: disable=unused-variable
        source_descriptions = ([
            dict({
                'num_records': random.randint(min_num_record, max_num_record)
            }, **common_attrs) for i in range(0, num_source_description)
        ])
        total_num_records = 0
        for source in source_descriptions:
            total_num_records += source['num_records']

        with self.create_pipeline() as p:
            res = (p
                   | beam.Create(source_descriptions)
                   | beam.ParDo(SyntheticSDFAsSource())
                   | beam.combiners.Count.Globally())
            assert_that(res, equal_to([total_num_records]))

    def test_create_value_provider_pipeline_option(self):
        # Verify that the runner can execute a pipeline when there are value
        # provider pipeline options
        # pylint: disable=unused-variable
        class FooOptions(PipelineOptions):
            @classmethod
            def _add_argparse_args(cls, parser):
                parser.add_value_provider_argument(
                    "--foo", help='a value provider argument', default="bar")

        RuntimeValueProvider.set_runtime_options({})

        with self.create_pipeline() as p:
            assert_that(p | beam.Create(['a', 'b']), equal_to(['a', 'b']))

    def _test_pack_combiners(self, assert_using_counter_names):
        counter = beam.metrics.Metrics.counter('ns', 'num_values')

        def min_with_counter(values):
            counter.inc()
            return min(values)

        def max_with_counter(values):
            counter.inc()
            return max(values)

        class PackableCombines(beam.PTransform):
            def annotations(self):
                return {python_urns.APPLY_COMBINER_PACKING: b''}

            def expand(self, pcoll):
                assert_that(
                    pcoll
                    | 'PackableMin' >> beam.CombineGlobally(min_with_counter),
                    equal_to([10]),
                    label='AssertMin')
                assert_that(
                    pcoll
                    | 'PackableMax' >> beam.CombineGlobally(max_with_counter),
                    equal_to([30]),
                    label='AssertMax')

        with self.create_pipeline() as p:
            _ = p | beam.Create([10, 20, 30]) | PackableCombines()

        res = p.run()
        res.wait_until_finish()

        packed_step_name_regex = (
            r'.*Packed.*PackableMin.*CombinePerKey.*PackableMax.*CombinePerKey.*'
            + 'Pack.*')

        counters = res.metrics().query(
            beam.metrics.MetricsFilter())['counters']
        step_names = set(m.key.step for m in counters)
        pipeline_options = p._options
        if assert_using_counter_names:
            if pipeline_options.view_as(StandardOptions).streaming:
                self.assertFalse(
                    any([
                        re.match(packed_step_name_regex, s) for s in step_names
                    ]))
            else:
                self.assertTrue(
                    any([
                        re.match(packed_step_name_regex, s) for s in step_names
                    ]))

    def test_pack_combiners(self):
        self._test_pack_combiners(assert_using_counter_names=True)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
