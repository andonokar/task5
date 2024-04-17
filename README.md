# task5
The purpose of task is to absorb data via streaming, save into postgres, clean and transform the data
## How it's done
The postgres image has pg_cron extension, which runs the processing functions after a certain time
cleaning not processed files every 10 minutes
creates the advertiser transformation every hour
## How to run
Just run the docker compose in the repository, and it's already working, don't forget to change the volume
persistence for postgres to match your local files.
## What's being done
A spark image absorbs data from a public kafka and then saves via streaming into postgres, then postgres itself takes
care of the rest