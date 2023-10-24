#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/
#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays
#importing some Python libraries
# --> add your Python code here
import pymongo

#did mongo connection here but deleted because i wasnt sure if it was private info

# Define a function to connect to the MongoDB database
def connectDataBase():
    try:
        # Create a database connection object using pymongo
        client = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
        db = client[DB_NAME]
        return db
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def createDocument(col, docId, docText, docTitle, docDate, docCat):
    try:
        # Create a dictionary to count how many times each term appears in the document
        term_counts = {}
        terms = docText.split()
        for term in terms:
            # Use space " " as the delimiter character for terms and lowercase them
            term = term.lower()
            term_counts[term] = term_counts.get(term, 0) + 1

        # Create a list of dictionaries to include term objects
        term_objects = [{"term": term, "count": count} for term, count in term_counts.items()]

        # Produce a final document as a dictionary including all the required document fields
        document = {
            "docId": docId,
            "docText": docText,
            "docTitle": docTitle,
            "docDate": docDate,
            "docCat": docCat,
            "terms": term_objects
        }

        # Insert the document
        col.insert_one(document)
    except Exception as e:
        print(f"Error creating document: {e}")

def deleteDocument(col, docId):
    try:
        # Delete the document from the database
        col.delete_one({"docId": docId})
    except Exception as e:
        print(f"Error deleting document: {e}")

def updateDocument(col, docId, docText, docTitle, docDate, docCat):
    try:
        # Delete the document
        deleteDocument(col, docId)

        # Create the document with the same id
        createDocument(col, docId, docText, docTitle, docDate, docCat)
    except Exception as e:
        print(f"Error updating document: {e}")

def getIndex(col):
    try:
        index = {}

        # Query the database to return the documents where each term occurs with their corresponding count
        cursor = col.find({}, {"_id": 0, "terms": 1})
        for document in cursor:
            for term_info in document["terms"]:
                term = term_info["term"]
                count = term_info["count"]
                if term not in index:
                    index[term] = []
                index[term].append(f'Document:{document["docId"]}:{count}')

        return index
    except Exception as e:
        print(f"Error getting index: {e}")


