#-------------------------------------------------------------------------
# AUTHOR: JOSE NUÑEZ
# FILENAME: DB_CONNECTION.PY
# SPECIFICATION: Database connection and CRUD operations
# FOR: CS 4250- Assignment #1
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/
#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH
#AS numpy OR pandas. You have to work here only with
# standard arrays
#importing some Python libraries
import psycopg2

# Define your database connection parameters
DB_NAME = 'your_db_name'
DB_USER = 'your_db_user'
DB_PASSWORD = 'your_db_password'
DB_HOST = 'your_db_host'


def connectDataBase():
    try:
        # Create a database connection object using psycopg2
        conn = psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        return None

def createCategory(conn, cur, catId, catName):
    try:
        # Insert a category into the database
        cur.execute("INSERT INTO categories (cat_id, cat_name) VALUES (%s, %s)", (catId, catName))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating category: {e}")

def createDocument(conn, cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1. Get the category id based on the informed category name
        cur.execute("SELECT cat_id FROM categories WHERE cat_name = %s", (docCat,))
        cat_id = cur.fetchone()[0] if cur.rowcount > 0 else None

        if cat_id is not None:
            # 2. Insert the document into the database
            num_chars = len(docText.replace(' ', '').replace(',', '').lower())
            cur.execute("INSERT INTO documents (doc_id, doc_text, doc_title, doc_date, cat_id, num_chars) VALUES (%s, %s, %s, %s, %s, %s)",
                        (docId, docText, docTitle, docDate, cat_id, num_chars))

            # 3. Update the potential new terms
            terms = docText.split()
            for term in terms:
                # Remove punctuation marks
                term = term.lower().strip('.,!?()[]{}<>:;"\'')
                # 3.2 and 3.3: Check if the term already exists in the database
                cur.execute("SELECT term_id FROM terms WHERE term_text = %s", (term,))
                if cur.rowcount == 0:
                    # Term does not exist, insert it into the database
                    cur.execute("INSERT INTO terms (term_text) VALUES (%s)", (term,))

            # 4. Update the index
            term_counts = {}
            for term in terms:
                term = term.lower().strip('.,!?()[]{}<>:;"\'')
                term_counts[term] = term_counts.get(term, 0) + 1

            for term, count in term_counts.items():
                cur.execute("INSERT INTO term_index (term_id, doc_id, term_count) VALUES ((SELECT term_id FROM terms WHERE term_text = %s), %s, %s)",
                            (term, docId, count))

            conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating document: {e}")

def deleteDocument(conn, cur, docId):
    try:
        # 1. Query the index based on the document to identify terms
        cur.execute("SELECT term_id, term_count FROM term_index WHERE doc_id = %s", (docId,))
        term_info = cur.fetchall()

        for term_id, term_count in term_info:
            # 1.1 For each term identified, delete its occurrences in the index for that document
            cur.execute("DELETE FROM term_index WHERE term_id = %s AND doc_id = %s", (term_id, docId))

            # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
            cur.execute("SELECT COUNT(*) FROM term_index WHERE term_id = %s", (term_id,))
            count = cur.fetchone()[0]
            if count == 0:
                # If no more occurrences, delete the term from the database
                cur.execute("DELETE FROM terms WHERE term_id = %s", (term_id,))

        # 2. Delete the document from the database
        cur.execute("DELETE FROM documents WHERE doc_id = %s", (docId,))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Error deleting document: {e}")

def updateDocument(conn, cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1. Delete the document
        deleteDocument(conn, cur, docId)

        # 2. Create the document with the same id
        createDocument(conn, cur, docId, docText, docTitle, docDate, docCat)
    except psycopg2.Error as e:
        print(f"Error updating document: {e}")

def getIndex(conn, cur):
    try:
        index = {}

        # Query the database to return the documents where each term occurs with their corresponding count
        cur.execute("SELECT t.term_text, ti.doc_id, ti.term_count FROM term_index ti JOIN terms t ON ti.term_id = t.term_id")
        index_data = cur.fetchall()

        for term_text, doc_id, term_count in index_data:
            if term_text not in index:
                index[term_text] = []
            index[term_text].append(f'Document:{doc_id}:{term_count}')

        return index
    except psycopg2.Error as e:
        print(f"Error getting index: {e}")
# filling table, didnt have time to make full database using menu:
conn = connectDataBase()
if conn:
    cur = conn.cursor()
    
    # Create categories
    createCategory(conn, cur, '1', 'Sports')
    createCategory(conn, cur, '2', 'Science')
    
    # Create documents
    createDocument(conn, cur, '1', 'Baseball is a sport.', 'Baseball', '2023-10-11', 'Sports')
    createDocument(conn, cur, '2', 'Basketball is another sport.', 'Basketball', '2023-10-12', 'Sports')
    createDocument(conn, cur, '3', 'Physics is a branch of science.', 'Physics', '2023-10-13', 'Science')
    
    print("Index before update:")
    print(getIndex(conn, cur))
    
    # Update a document
    updateDocument(conn, cur, '2', 'Basketball is a popular sport.', 'Basketball', '2023-10-12', 'Sports')
    
    print("Index after update:")
    print(getIndex(conn, cur))
    
    # Delete a document
    deleteDocument(conn, cur, '1')
    
    print("Index after deletion:")
    print(getIndex(conn, cur))
    
    cur.close()
    conn.close()
