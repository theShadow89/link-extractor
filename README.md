### Prerequisite

- docker
- python

### Pipeline Setup

Create virtual env and install all requirement

```shell
python3 -m venv pipeline_env
source pipeline_env/bin/activate
pip install -r requirements.txt
```

Run postgress container. the script will start a container and will init it with correct tables

```shell
docker compose up
```

Download segment data using the script. The script will download the data into `data/segments` folder

```shell
chamod +x download_segments_data.sh
./download_segments_data
```

### Pipeline Execution

In order to run the pipeline exec the `pipeline.py` script and provide the folder where segment data was downloaded

```shell
python pipline.py data/segments
```

The execution will take a while to complete.