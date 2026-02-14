# specify base image
FROM python:3.11-slim

# set working directory
WORKDIR /app

# copy requirements file
COPY requirements.txt .

# install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the bot code and data into the container
COPY . .

CMD [ "python", "antiscam.py" ]
