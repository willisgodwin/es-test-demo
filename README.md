# es-test-demo
This repo will hold the code for my ES lightening talk

Run Instructions

Download the ES docker image 
1. Instructions [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html)
2. change       - xpack.security.enabled=true to       - xpack.security.enabled=false
  - this turns off security on your local elasticsearch cluster [reference](https://levelup.gitconnected.com/how-to-run-elasticsearch-8-on-docker-for-local-development-401fd3fff829)
  - Another [reference](https://discuss.elastic.co/t/how-do-i-disable-x-pack-security-on-the-elasticsearch-5-2-2-docker-image/78183/3) for doing this via docker run
3. start the container
4. once it starts the user should be able to go to http://localhost:9200 and see the tag_line "You know, for search"

Clone this repo
1. clone the repo
2. cd into the folder where you cloned the repo
3. create a virtualenv
4. activate the virtualenv
5. pip install -r requirements.txt
6. python ./main.py
