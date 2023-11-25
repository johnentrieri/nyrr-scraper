import requests
import boto3
import hashlib
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup

def scrape():
    
    # Create empty array of race objects
    races = []

    # GET Request to NYRR 2024 Race Calendar
    r = requests.get('https://www.nyrr.org/fullraceyearindex?year=2024')

    # Exit if GET request failed
    if (r.status_code != 200):
        print("HTTP Request Failed")
        return(races)

    # Instantiate Beautful Soup Object for parsing
    soup = BeautifulSoup(r.text, features="html.parser")

    # Find all races listed in raw HTML
    rawRaces = soup.find_all("div", class_ = "index_listing__inner")

    for rawRace in rawRaces:
        
        # Populate Race Object
        race = {}
        race['date'] = rawRace.find("div", class_ = "index_listing__date").text.strip()
        race['time'] = rawRace.find("div", class_ = "index_listing__time").text.strip()
        race['title'] = rawRace.find("div", class_ = "index_listing__title").text.strip()
        race['status'] = rawRace.find("div", class_ = "index_listing__status").text.strip()
        race['location'] = rawRace.find("div", class_ = "index_listing__location").text.strip()
        race['raceID'] = hashlib.md5(race['title'].encode('utf-8')).hexdigest()

        # Add to array of races
        races.append(race)

    # Return array of races
    return(races)

def compare(old,new):

    # Empty array of changes
    changes = []

    # Loop through each race in the 'new' array
    for newRace in new:

        # Flag for race existing in 'old' array
        raceExists = False

        # Loop through each race in the 'old' array
        for oldRace in old:

            # Compare Race Titles
            if (newRace['title'] == oldRace['title']):

                raceExists = True

                # Compare Race Date
                if (newRace['date'] != oldRace['date']):
                    change = {}
                    change['subject'] = "Race Date Change"
                    change['message'] = ""
                    change['message'] += oldRace['title']
                    change['message'] += ": Date changed from "
                    change['message'] += oldRace['date']
                    change['message'] += " to "
                    change['message'] += newRace['date']
                    changes.append(change)

                # Compare Race Time
                if (newRace['time'] != oldRace['time']):
                    change = {}
                    change['subject'] = "Race Time Change"
                    change['message'] = ""
                    change['message'] += oldRace['title']
                    change['message'] += ": Time changed from "
                    change['message'] += oldRace['time']
                    change['message'] += " to "
                    change['message'] += newRace['time']
                    changes.append(change)

                # Compare Race Location
                if (newRace['location'] != oldRace['location']):
                    change = {}
                    change['subject'] = "Race Location Change"
                    change['message'] = ""
                    change['message'] += oldRace['title']
                    change['message'] += ": Location changed from "
                    change['message'] += oldRace['location']
                    change['message'] += " to "
                    change['message'] += newRace['location']
                    changes.append(change)

                # Compare Race Status
                if (newRace['status'] != oldRace['status']):
                    change = {}
                    change['subject'] = "Race Status Change"
                    change['message'] = ""
                    change['message'] += oldRace['title']
                    change['message'] += ": Status changed from "
                    change['message'] += oldRace['status']
                    change['message'] += " to "
                    change['message'] += newRace['status']
                    changes.append(change)

        # New Race Found          
        if (not(raceExists)):
            change = {}
            change['subject'] = "New Race Posted"
            change['message'] = ""
            change['message'] += "New Race: "
            change['message'] += newRace['title']
            change['message'] += " on "
            change['message'] += newRace['date']
            change['message'] += " at "
            change['message'] += newRace['time']
            change['message'] += " in "
            change['message'] += newRace['location']
            change['message'] += " ["
            change['message'] += newRace['status']
            change['message'] += "]"
            changes.append(change)

    # Return array of changes
    return(changes)


def uploadRaces(raceArray):

    # Connect to DynamoDB database
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.environ.get("AWS_REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

    # Get NYRR Races Table
    table = dynamodb.Table('nyrr-races')

    # Scan for all items in database
    items = table.scan()

    # Remove all races withing database
    if 'Items' in items.keys():
        for race in items['Items']:
            table.delete_item( Key={ 'raceID' : race['raceID'] } )

    # Re-add all races to database
    for race in raceArray:
            table.put_item( Item = race )

def downloadRaces():

    # Empty race array
    races = []

    # Connect to DynamoDB database
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=os.environ.get("AWS_REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

    # Get NYRR Races Table
    table = dynamodb.Table('nyrr-races')

    # Scan for all items in database
    items = table.scan()

    # Add database item to race array
    if 'Items' in items.keys():
        for race in items['Items']:
            races.append(race)

    return(races)

def notify(subject, message):

    # Connect to SNS Client
    sns = boto3.client(
        'sns',
        region_name=os.environ.get("AWS_REGION"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

    # Publish Subject & Message to NYRR Races Topic
    response = sns.publish(
        TopicArn=os.environ.get("NYRR_TOPIC_ARN"),
        Subject=subject,
        Message=message
    )

# Take environment variables from .env
load_dotenv()

# Pull Previous Race Info from Database
oldRaceList = downloadRaces()

# Scrape Current Race Info from NYRR Site
newRaceList = scrape()

# Compare Race Lists
changeList = compare(oldRaceList, newRaceList)

# If identical, do nothing
if (len(changeList) == 0):
    print("No changes to report")

# If there are changes, overwrite database
else:

    uploadRaces(newRaceList)
    for change in changeList:
        print(change['subject'] + " - " + change['message'])
        print()

        notify(change['subject'], change['message'])