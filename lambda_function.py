import nyrr.nyrrscraper as nyrrscraper

def handler(event, context):
    # Take environment variables from .env
    nyrrscraper.load_dotenv()

    # Pull Previous Race Info from Database
    oldRaceList = nyrrscraper.downloadRaces()

    # Scrape Current Race Info from NYRR Site
    newRaceList = nyrrscraper.scrape()

    # Compare Race Lists
    changeList = nyrrscraper.compare(oldRaceList, newRaceList)

    # If identical, do nothing
    if (len(changeList) == 0):
        print("No changes to report")

    # If there are changes, overwrite database
    else:

        nyrrscraper.uploadRaces(newRaceList)
        for change in changeList:
            print(change['subject'] + " - " + change['message'])
            print()

            nyrrscraper.notify(change['subject'], change['message'])