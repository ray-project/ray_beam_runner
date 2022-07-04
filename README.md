# Ray-based Apache Beam Runner #

This is a WIP proof of concept implementation undergoing frequent breaking changes and should not be used in production.

## Build ##

*Prerequisite*: you need to have docker installed on your machine.

Run `build.sh` to generate python wheels and build the Ray Beam runner image. When successful, you will have `raybeamrunner:latest` and `raybeamrunner-buildwheel:latest` two images generated.
