# google-cloud-workflow-emulator

Unofficial [Google Cloud Workflow](https://cloud.google.com/workflows) emulator.

**WARNING: This product is alpha quality** 

## Install

```console
$ go install github.com/karupanerura/google-cloud-workflow-emulator/cmd/google-cloud-workflow-emulator@latest
```

TODO: add pre-built binary

## Usage


```console
# Execute Workflow
$ google-cloud-workflow-emulator -f ./example/sample.yaml --args '{}'

# Start Workflow Execution API emulator (request `POST http://localhost:8080/v1/projects/anything/locations/anything/workflows/anything/executions` to execute)
$ google-cloud-workflow-emulator -f ./example/sample.yaml -l 127.0.0.1:8080
```
