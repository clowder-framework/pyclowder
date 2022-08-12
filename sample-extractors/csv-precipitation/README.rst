A simple extractor that can execute binary programs to generate image previews, thumbnails, as well as previews. To
use this extractor you will need to create a new Dockerfile that sets environment variables, a packages.apt file that
lists all the packages that need to be installed, and an extractor_info.json that describes the extractor.

For example the following files will create an extractor that can handle audio files. To build the extractor you can
use `docker build -t clowder/extractor-audio`. You can now use the extractor in your clowder environment. For example
if you used the docker-compose file to start clowder yo can add this extractor to the running stack using
`docker run --rm --network clowder --link rabbitmq --link clowder clowder/extractor-audio` when you now upload an
audio file it will automatically create a thumbnail, image preview and audio preview.

Dockerfile

.. code-block:: Dockerfile

    FROM clowder/pyclowder:onbuild

    ENV RABBITMQ_QUEUE="ncsa.audio.preview" \
        IMAGE_BINARY="/usr/bin/sox" \
        IMAGE_TYPE="png" \
        IMAGE_THUMBNAIL_COMMAND="@BINARY@ --magic @INPUT@ -n spectrogram -r -x 225 -y 200 -o @OUTPUT@" \
        IMAGE_PREVIEW_COMMAND="@BINARY@ --magic @INPUT@ -n spectrogram -x 800 -Y 600 -o @OUTPUT@" \
        PREVIEW_BINARY="/usr/bin/sox" \
        PREVIEW_TYPE="mp3" \
        PREVIEW_COMMAND="@BINARY@ --magic @INPUT@ @OUTPUT@"


packages.apt

.. code-block::

    libsox-fmt-mp3 sox

extractor_info.json

.. code-block:: json

    {
       "@context":"http://clowder.ncsa.illinois.edu/contexts/extractors.jsonld",
       "name":"ncsa.audio.preview",
       "version":"1.0",
       "description":"Creates thumbnail and image previews of Audio files.",
       "author":"Rob Kooper <kooper@illinois.edu>",
       "contributors":[],
       "contexts":[],
       "repository":[
          {
             "repType":"git",
             "repUrl":"https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/extractors-core.git"
          },
          {
             "repType":"docker",
             "repUrl":"clowder/extractors-audio-preview"
          }
       ],
       "external_services":[],
       "process":{
          "file":[
             "audio/*"
          ]
       },
       "dependencies":[
          "sox-fmt-mp3",
          "sox"
       ],
       "bibtex":[]
    }
