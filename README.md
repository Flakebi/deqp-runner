# Deqp Runer
Run the Vulkan Conformance Test Suite in parallel and robustly.

This is fork/rewrite of the mesa [parallel deqp runner](https://gitlab.freedesktop.org/mesa/parallel-deqp-runner).

## Features

- Run tests in parallel
- Recover from crashes (e.g. failing asserts)
- Recover from timeouts
- Save results even on fatal errors
- Cross-platform
- Automatically retry failing tests
- Automatically bisect failures if they depend on a combination of tests
- Save a junit compatible xml file of the results

## Usage
Create a `testlist.txt` with one deqp test name per line. Run with
```bash
deqp-runner -t testlist.txt -- ./deqp-vk --deqp-caselist-file
```

A more complicated command:
```bash
deqp-runner -t testlist.txt \
	--timeout 10 \
	--jobs 2 \
	--shuffle \
	--no-progress \
	--start 50 \
	-- ./deqp-vk \
	--deqp-log-flush=disable \
	--deqp-log-images=disable \
	--deqp-log-shader-sources=disable \
	--deqp-surface-width=256 \
	--deqp-surface-height=256 \
	--deqp-surface-type=pbuffer \
	--deqp-gl-config-name=rgba8888d24s8ms0 \
	--deqp-caselist-file
```

## Build
Install [Rust](https://rust-lang.org) (preferred installation method is [rustup](https://rustup.rs))
and build with `cargo build --release`.

## License
Licensed under either of

 * [Apache License, Version 2.0](LICENSE-APACHE)
 * [MIT license](LICENSE-MIT)

at your option.
