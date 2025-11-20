#!/bin/bash
# Bash script to send systemd notifications to Slack

function usage {
    programName=$0
    echo "description: use this script to post systemd service failure message to Slack channel"
    echo "usage: $programName -s \"service name\""
    echo "	-s    the systemd service name e.g. nginx"
    exit 1
}

# Get service name from options
while getopts ":s:" opt; do
  case $opt in
    s)
      SERVICE_NAME=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

#echo "service name is '${SERVICE_NAME}'"

if [[ ! "${SERVICE_NAME}" ]]; then
    echo "Service name is required"
    usage
fi

# Edit the following variables to match your requirements
SLACK_HOOK_FILE=/opt/middleman/slack_web_hook.txt
SLACK_WEBHOOK_URL=$(cat ${SLACK_HOOK_FILE})
SLACK_CHANNEL="#general"
SLACK_USERNAME="Notification Bot"
SLACK_ICON=":zap:"
SLACK_COLOR="danger"
SLACK_TITLE="Service $SERVICE_NAME failed on $(hostname)"
SLACK_PRETEXT="Service $SERVICE_NAME failed"
SLACK_TEXT="$(systemctl status $SERVICE_NAME)"
#SLACK_TEXT="testing service \"potato\""
SLACK_FOOTER="Notification Bot at $(hostname) on $(date)"
# End of variables

SLACK_ATTACHMENT='[{"fallback": "'"$SLACK_MESSAGE"'", "color": "'"$SLACK_COLOR"'", "title": "'"$SLACK_TITLE"'", "title_link": "'"$SLACK_TITLE_LINK"'", "pretext": "'"$SLACK_PRETEXT"'", "text": "'"$SLACK_TEXT"'", "footer": "'"$SLACK_FOOTER"'", "footer_icon": "'"$SLACK_FOOTER_ICON"'"}]'

# Send notification to Slack
curl -X POST --data-urlencode 'payload={"channel": "'"$SLACK_CHANNEL"'", "username": "'"$SLACK_USERNAME"'", "icon_emoji": "'"$SLACK_ICON"'", "attachments": '"$SLACK_ATTACHMENT"'}' $SLACK_WEBHOOK_URL

# Exit with success code
exit 0
