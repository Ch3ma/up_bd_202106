#gcloud init in a terminal to set configuration, check if correct project and user

if [ ! -n "$1" ]
then
  echo "Must provide script to deploy"
  exit 1
fi

if [ ! -n "$2" ]
then
  echo "Must provide an entry point (function inside the code)"
  exit 1
fi

FUNCTION_FILE="functions/"$1
REQ_FILE="functions/"$1"/requirements.txt"

if [ ! -z "$3" ] #update requirements with PipFile
then
  if [ "$3" == "update" ]
    then
    echo "Updating requirements.txt with PipFile"
    pipenv lock -r > requirements.txt
    mv requirements.txt $FUNCTION_FILE/requirements.txt
  fi
fi

if [ ! -f "$REQ_FILE"  ] #create requirements the very first time
then
  echo "First run, creating requirements.txt from Pipfile"
  #file="functions/"$1
  pipenv lock -r > requirements.txt
  mv requirements.txt $FUNCTION_FILE/requirements.txt
fi

#cd $file
cd $FUNCTION_FILE

gcloud functions deploy $1 --entry-point $2 --runtime python38 --trigger-http --allow-unauthenticated --memory=512MB
echo $REQ_FILE

