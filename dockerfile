#Deriving the latest base image
FROM python:latest

#Labels as key value pair
LABEL Maintainer="mhmdzaky"

# Any working directory can be chosen as per choice like '/' or '/home' etc
# i have chosen /usr/app/src
WORKDIR /usr/app/src

#to COPY the remote file at working directory in container
# COPY practice_case1.py ./
# COPY data-fellowship7-83ba187356c9.json ./
COPY . .
# Now the structure looks like this '/usr/app/src/test.py'

# RUN pip install --no-cache-dir --upgrade pip && 
RUN pip install --upgrade google-cloud-storage

#CMD instruction should be used to run the software
#contained by your image, along with any arguments.

CMD [ "python", "./practice_case1.py"]