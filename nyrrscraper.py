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
                    
                    changeString = ""
                    changeString += oldRace['title']
                    changeString += ": Date changed from "
                    changeString += oldRace['date']
                    changeString += " to "
                    changeString += newRace['date']
                    changes.append(changeString)

                # Compare Race Time
                if (newRace['time'] != oldRace['time']):
                    changeString = ""
                    changeString += oldRace['title']
                    changeString += ": Time changed from "
                    changeString += oldRace['time']
                    changeString += " to "
                    changeString += newRace['time']
                    changes.append(changeString)

                # Compare Race Location
                if (newRace['location'] != oldRace['location']):
                    changeString = ""
                    changeString += oldRace['title']
                    changeString += ": Location changed from "
                    changeString += oldRace['location']
                    changeString += " to "
                    changeString += newRace['location']
                    changes.append(changeString)

                # Compare Race Status
                if (newRace['status'] != oldRace['status']):
                    changeString = ""
                    changeString += oldRace['title']
                    changeString += ": Status changed from "
                    changeString += oldRace['status']
                    changeString += " to "
                    changeString += newRace['status']
                    changes.append(changeString)

        # New Race Found          
        if (not(raceExists)):
            changeString = ""
            changeString += "New Race: "
            changeString += newRace['title']
            changeString += " on "
            changeString += newRace['date']
            changeString += " at "
            changeString += newRace['time']
            changeString += " in "
            changeString += newRace['location']
            changeString += " ["
            changeString += newRace['status']
            changeString += "]"
            changes.append(changeString)

    # Return array of change strings
    return(changes)


def uploadRaces(raceArray):

    # Connect to DynamoDB database
    dynamodb = boto3.resource(
        'dynamodb',
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))

    # Get NYRR Races Table
    table = dynamodb.Table('nyrr-races')

    # Loop through each race in array
    for race in raceArray:

        # Attempt to find item by raceID in database
        item = table.get_item( Key={ 'raceID' : race['raceID'] })

        # If raceID already exists, skip
        if 'Item' in item.keys():
            continue

        # If raceID not found, add race to database
        else:
            table.put_item( Item = race )

def downloadRaces():

    # Empty race array
    races = []

    # Connect to DynamoDB database
    dynamodb = boto3.resource(
        'dynamodb',
        region_name="us-east-1",
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

# Take environment variables from .env
load_dotenv()

oldRaceList = downloadRaces()
newRaceList = scrape()
changeList = compare(oldRaceList, newRaceList)
uploadRaces(newRaceList)

for change in changeList:
    print(change)
    print()
