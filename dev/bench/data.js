window.BENCHMARK_DATA = {
  "lastUpdate": 1670968927426,
  "repoUrl": "https://github.com/ray-project/ray_beam_runner",
  "entries": {
    "Python Benchmark with pytest-benchmark": [
      {
        "commit": {
          "author": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "committer": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "id": "f04d7504b36bafdcaceea29b3f9ddc77a903c306",
          "message": "[WIP]add benchmark for performance testing",
          "timestamp": "2022-07-31T18:48:55Z",
          "url": "https://github.com/ray-project/ray_beam_runner/pull/36/commits/f04d7504b36bafdcaceea29b3f9ddc77a903c306"
        },
        "date": 1660709780974,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 8997125.421984171,
            "unit": "iter/sec",
            "range": "stddev: 8.63711916609269e-8",
            "extra": "mean: 111.14661106717445 nsec\nrounds: 23981"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "committer": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "id": "52475eb3adb230c80e1149b537ed807c03866fea",
          "message": "[WIP]add benchmark for performance testing",
          "timestamp": "2022-09-13T18:00:58Z",
          "url": "https://github.com/ray-project/ray_beam_runner/pull/36/commits/52475eb3adb230c80e1149b537ed807c03866fea"
        },
        "date": 1663129910823,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 11456008.726675073,
            "unit": "iter/sec",
            "range": "stddev: 1.2537819684740362e-8",
            "extra": "mean: 87.29043629931957 nsec\nrounds: 57472"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "committer": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "id": "199e70fd7608af498697d382ec19858c3867077b",
          "message": "Let driver own pcollections",
          "timestamp": "2022-09-13T18:00:58Z",
          "url": "https://github.com/ray-project/ray_beam_runner/pull/41/commits/199e70fd7608af498697d382ec19858c3867077b"
        },
        "date": 1663686091805,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 10735320.889289128,
            "unit": "iter/sec",
            "range": "stddev: 2.475066680991662e-8",
            "extra": "mean: 93.15045263321669 nsec\nrounds: 53476"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "committer": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "id": "69c345b6672559508f201c9e7f354447f1fcd59c",
          "message": "Install ray nightly for ci",
          "timestamp": "2022-10-11T18:40:57Z",
          "url": "https://github.com/ray-project/ray_beam_runner/pull/43/commits/69c345b6672559508f201c9e7f354447f1fcd59c"
        },
        "date": 1665588196902,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 9707834.570411775,
            "unit": "iter/sec",
            "range": "stddev: 2.3557475891637778e-8",
            "extra": "mean: 103.00958393419451 nsec\nrounds: 47168"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "committer": {
            "name": "ray-project",
            "username": "ray-project"
          },
          "id": "69c345b6672559508f201c9e7f354447f1fcd59c",
          "message": "Install ray nightly for ci",
          "timestamp": "2022-10-11T18:40:57Z",
          "url": "https://github.com/ray-project/ray_beam_runner/pull/43/commits/69c345b6672559508f201c9e7f354447f1fcd59c"
        },
        "date": 1666132859126,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 4058835.8674763166,
            "unit": "iter/sec",
            "range": "stddev: 4.3970726832526886e-7",
            "extra": "mean: 246.37606265704338 nsec\nrounds: 144928"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ilion.beyst@gmail.com",
            "name": "iasoon",
            "username": "iasoon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c713097bede5704bb2b1dc5ee5f5430e028dc3c6",
          "message": "save benchmarks on master branch only (#50)\n\nBenchmarks should only be saved on builds for the master branch (otherwise a non-accepted PR would overwrite the current stats).",
          "timestamp": "2022-11-02T09:12:44-07:00",
          "tree_id": "c45c7e073c7ceb2f1488f6c55d4ee96befd8f9b8",
          "url": "https://github.com/ray-project/ray_beam_runner/commit/c713097bede5704bb2b1dc5ee5f5430e028dc3c6"
        },
        "date": 1667405676354,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 3285733.3772989763,
            "unit": "iter/sec",
            "range": "stddev: 5.812641206367983e-7",
            "extra": "mean: 304.34605768957607 nsec\nrounds: 169492"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "pabloem@users.noreply.github.com",
            "name": "Pablo",
            "username": "pabloem"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "33399596c095a31424f4ff14611377b7ff4e65ba",
          "message": "Merge pull request #53 from flyingImer/master\n\nchore: removes Ray nightly from dependency in favor of 2.1 release",
          "timestamp": "2022-11-09T09:09:40-08:00",
          "tree_id": "81a2b6b1f228cb3752ef70fbd28902f0e9191b0a",
          "url": "https://github.com/ray-project/ray_beam_runner/commit/33399596c095a31424f4ff14611377b7ff4e65ba"
        },
        "date": 1668013897209,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 2952267.9542873544,
            "unit": "iter/sec",
            "range": "stddev: 0.000004965933059671291",
            "extra": "mean: 338.7226415365773 nsec\nrounds: 71943"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ilion.beyst@gmail.com",
            "name": "iasoon",
            "username": "iasoon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5dd1eb4e6c877882a2ebc0644d0ba1b0a96a512e",
          "message": "Complete user-initiated SDF functionality (#52)\n\n* correctly set is_drain parameter\r\n\r\n* enable passing runner tests\r\n\r\n* Support deferred applications in drain mode\r\n\r\n* implement bundle finalization",
          "timestamp": "2022-11-10T15:29:56-05:00",
          "tree_id": "60e819824ef7ec0060967a431e8da3c5dbd34555",
          "url": "https://github.com/ray-project/ray_beam_runner/commit/5dd1eb4e6c877882a2ebc0644d0ba1b0a96a512e"
        },
        "date": 1668112282441,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 11973081.110939538,
            "unit": "iter/sec",
            "range": "stddev: 3.504231204294896e-8",
            "extra": "mean: 83.52069034980218 nsec\nrounds: 56819"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "valiantljk@gmail.com",
            "name": "jialin",
            "username": "valiantljk"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "956e7cfd93e751ffa4b9661a28fca47f19dfdf05",
          "message": "Support metrics aggregation in Ray runner (#56)\n\n* support non-user and element-wise metrics\r\n\r\n* unlocked all metrics related tests except progress metrics\r\n\r\n* word count example with metrics\r\n\r\n* fix module import\r\n\r\n* import module inside ray task\r\n\r\n* remove example\r\n\r\n* add word count metrics example and re-formatted code locally\r\n\r\nCo-authored-by: Jialin Liu <rootliu@amazon.com>",
          "timestamp": "2022-12-08T14:46:24-05:00",
          "tree_id": "09fbad05a8120716fbfa60b5d9cea533e9e3cdcd",
          "url": "https://github.com/ray-project/ray_beam_runner/commit/956e7cfd93e751ffa4b9661a28fca47f19dfdf05"
        },
        "date": 1670528881981,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 11395954.650823226,
            "unit": "iter/sec",
            "range": "stddev: 9.509123129109762e-9",
            "extra": "mean: 87.75043694370578 nsec\nrounds: 52913"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "rkenmi@gmail.com",
            "name": "rkenmi",
            "username": "rkenmi"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "be06442dfff1c437f45cffa3a59f85283b6a14ad",
          "message": "Register protobuf serializers (#58)\n\n* Add serialization helper for protobuf messages\r\n\r\n* Add licensing",
          "timestamp": "2022-12-13T17:00:31-05:00",
          "tree_id": "592c6d66cbe5c1e063e923da464136601d019086",
          "url": "https://github.com/ray-project/ray_beam_runner/commit/be06442dfff1c437f45cffa3a59f85283b6a14ad"
        },
        "date": 1670968926940,
        "tool": "pytest",
        "benches": [
          {
            "name": "benchmark/simple.py::test_simple_benchmark",
            "value": 11751211.950859495,
            "unit": "iter/sec",
            "range": "stddev: 1.83892957809635e-8",
            "extra": "mean: 85.097605607066 nsec\nrounds: 58140"
          }
        ]
      }
    ]
  }
}