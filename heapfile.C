#include "heapfile.h"
#include "error.h"

/******************************************************************************
 * File: heapfile.C
 * 
 * Purpose: This file implements the a file manager for heap files. It also provides
 *          a mechanism that allows to search a heap file for records that match
 *          a given filter.
 * 
 * Authors: Nate Colburn, Casey Strayer, Vasudha Khanna
 * Student IDs: ncolburn2, cstrayer, vkhanna7
 *****************************************************************************/

/**
 * Creates a new heap file with the specified file name. If the file already exists, 
 * it returns FILEEXISTS. If any operation fails (like file creation, opening, or 
 * allocating pages), the appropriate error status is returned.
 *
 * @param fileName - The name of the heap file to be created.
 * @return Status - Status information from the heap file creation process.
 **/
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.

        // Creating file and allocat9g header page
        status = db.createFile(fileName); // Create a DB level file
        if (status != OK) return status; // Check for errors in file creation
        status = db.openFile(fileName, file); // Open the file to initialize it
        if (status != OK) return status;
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) return status;

        // Initialize the values in the header page
        hdrPage = (FileHdrPage*) newPage; // Cast the page pointer to a header page
        hdrPageNo = newPageNo; // Store the page number of the header page
        strcpy(hdrPage->fileName, fileName.c_str()); // Set the file name in the header

        // Allocating the first data page of the file
        status = bufMgr->allocPage(file, newPageNo, newPage);
        if(status != OK) return status;
        newPage->init(newPageNo); // Initialize the page contents

        // Update the header page with the details of the data page
        hdrPage->pageCnt = 1; // Set the number of pages in the file
        hdrPage->recCnt = 0; // Set the number of records in the file
        hdrPage->firstPage = newPageNo; // Set the first page
        hdrPage->lastPage = newPageNo; // Set the last page

        // Unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, hdrPageNo, true); // Unpin the header page
        if(status != OK) return status;

        status = bufMgr->unPinPage(file, newPageNo, true); // Unpin the data page
        if(status != OK) return status;

        status = db.closeFile(file); // Close the file
        if(status != OK) return status;

        return OK;		
    }
    return (FILEEXISTS);
}

/**
 * Destroys the heap file with the specified file name.
 *
 * @param fileName - The name of the heap file to be destroyed.
 * @return Status - Status information from the heap file destruction process.
 **/
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

/**
 * Constructs a HeapFile object, opening an existing heap file and initializing
 * its header and first data page. If the file cannot be opened or any page operation
 * fails, the method sets an appropriate error status.
 *
 * @param fileName - The name of the heap file to open.
 * @param returnStatus - A reference to a Status variable that indicates the success 
 *                       or failure of the operation.
 * @return void
 **/
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // Open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK) // Open the file
    {
        status = filePtr->getFirstPage(headerPageNo); // Get the page number of the header page
        if (status != OK) {
            cerr << "getFirstPage failed\n";
            returnStatus = status;
            return;
        }

        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr); // Read the header page
        if (status != OK) {
            cerr << "readPage failed\n";
            returnStatus = status;
            return;
        }
        headerPage = (FileHdrPage*)pagePtr; // Cast the page pointer to a header page
        hdrDirtyFlag = false;
        curPageNo = headerPage->firstPage; // Get the page number of the first data page
		
		status = bufMgr->readPage(filePtr, curPageNo, curPage); // Read the first data page
        if (status != OK) {
            cerr << "reading current page failed\n";
            returnStatus = status;
            return;
        }
        // Initialize the current page details
        curDirtyFlag = false;
        curRec = NULLRID;		
    }
    else // Open file failed
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
    returnStatus = OK; // Return OK heapfile constructed successfully
}

/**
 * Destructs the HeapFile object, releasing any pinned pages and closing the file.
 * This method unpins the current data page (if pinned), unpins the header page, 
 * and closes the file. If there is an error, it is logged.
 *
 * @return void
 **/
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // See if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
    // Unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// Close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

/**
 * Returns the number of records in the heap file.
 *
 * @return int - The number of records in the heap file, as found in the header page.
 **/
const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

/**
 * Retrieves a record from the file based on the provided RID.
 * If the record is not on the currently pinned page, the current page is 
 * unpinned and the required page is read into the buffer pool and pinned.
 * A pointer to the retrieved record is returned via the rec parameter.
 *
 * @param rid - The RID of the record to be retrieved.
 * @param rec - A reference to the Record object where the retrieved record will be stored.
 * @return Status - The status of the retrieval process (OK if successful, error status otherwise).
 **/
const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // If the requested record is not on the current page, or the page is NULL, load the correct page
    if (curPage == NULL || rid.pageNo != curPageNo) {
        // Unpin the current page if necessary
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) {
                return status; // Return the error status if unpinning failed
            }
        }

        // Set the new page number and read the corresponding page into the buffer
        curPageNo = rid.pageNo;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            return status; // Return the error status if reading the page failed
        }
        curDirtyFlag = false;
    }

    // Retrieve the record from the current page
    status = curPage->getRecord(rid, rec);
    if (status != OK) {
        return status; // Return the error status if retrieving the record failed
    }

    // Update curRec to track the last returned record's RID
    curRec = rid;

    return OK; // Return OK if the record was successfully retrieved
}


HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


/**
 * Scans through records in a heap file and returns the next matching record 
 * according to the matching criteria.
 * 
 * This method iterates through the pages in the file, starting from the current page,
 * and retrieves records until it finds a match based on the matchRec method.
 * If no match is found, it continues to the next page or ends when there are no more pages.
 * 
 * @param outRid - A reference to the RID of the next matching record found.
 * @return Status - The status of the scanning process. It returns OK if a matching record is found,
 *                  FILEEOF if the end of the file is reached, or an error status in case of failure.
 */
const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    RID		tmpRid;
    int 	nextPageNo;
    Record      rec;

    nextPageNo = curPageNo;  // Initialize the next page number with the current page number

    while (true) {
        // Check if we've reached the end of the file
        if (nextPageNo == -1) {
            return FILEEOF;  // No more pages to scan
        }

        // Unpin the current page if it's not NULL before reading the next page
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;  // Return the error if unpinning fails
        }

        // Read the next page into the buffer pool
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if (status != OK) return status;

        // Update current page details
        curPageNo = nextPageNo;
        curDirtyFlag = false;  // The page is clean after being read

        // If we haven't processed any records yet, get the first record
        if (curRec.pageNo == NULLRID.pageNo && curRec.slotNo == NULLRID.slotNo) {
            status = curPage->firstRecord(nextRid);
            if (status != OK && status != NORECORDS) return status;
        }

        // If there are records to check on the page
        if (status != NORECORDS) {
            // If we are not at the first record, get the next record
            if (curRec.pageNo != NULLRID.pageNo || curRec.slotNo != NULLRID.slotNo) {
                status = curPage->nextRecord(curRec, nextRid);
                tmpRid = nextRid;  // Temporarily store the next record ID
            }

            // Return an error if we encounter an issue other than reaching the end of the page
            if (status != OK && status != ENDOFPAGE) {
                return status;
            }

            // If we successfully found a valid record, check it
            if (status == OK) {
                while (true) {
                    // Retrieve the record's data
                    status = curPage->getRecord(nextRid, rec);
                    if (status != OK) return status;

                    curRec = nextRid;  // Update the current record ID

                    // If the record matches the search, output it
                    if (matchRec(rec)) {
                        outRid = nextRid;  // Return the matching record ID
                        return OK;  // Successfully found a match
                    }

                    tmpRid = nextRid;  // Update the temporary RID for the next search

                    // Move to the next record on the page
                    status = curPage->nextRecord(tmpRid, nextRid);
                    if (status == ENDOFPAGE) {
                        curRec = NULLRID;  // Reset the current record if we're at the end of the page
                        break;  // No more records on this page, move to the next
                    }
                }
            } else {
                curRec = NULLRID;  // No more records on the current page
            }
        }

        // Get the next page number
        status = curPage->getNextPage(nextPageNo);
        if (status != OK) return status;
    }

    return OK;  // Return OK if scanning completes successfully
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file. 
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

/**
 * Inserts the record rec into the file, returning the RID of the inserted record in outRid.
 * If the current page is NULL, the method reads the last page into the buffer. If the record cannot fit on
 * the current page, a new page is allocated and properly linked, and the record is inserted there.
 *
 * @param rec - The record to be inserted.
 * @param outRid - A reference to the RID where the inserted record's location will be stored.
 * @return Status - The status of the insertion process (OK if successful, or an error status).
 */
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status;
    bool unpinstatus = false;
    RID rid;

    // Check for very large records
    if ((unsigned int) rec.length > PAGESIZE - DPFIXED)
    {
        // Will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // If the current page is NULL, initialize it to the last page
    if (curPage == NULL)
    {
        newPageNo = headerPage->lastPage;
        if (newPageNo != -1)
        {
            status = bufMgr->readPage(filePtr, newPageNo, curPage);
            if (status != OK) return status;

            curPageNo = newPageNo;
            curDirtyFlag = false;
        }
    }

    // Loop to handle insertion or allocation of new pages
    while (true)
    {
        // Attempt to insert the record on the current page
        status = curPage->insertRecord(rec, rid);

        if (status == OK)
        {
            // Record successfully inserted, perform bookkeeping
            outRid = rid;
            curRec = rid;
            curDirtyFlag = true;

            // Update header information
            headerPage->recCnt++;
            hdrDirtyFlag = true;

            return OK;
        }

        if (status != NOSPACE)
        {
            // Return any other error
            return status;
        }

        // If there's no space, allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;

        // Initialize the new page and link it to the file
        newPage->init(newPageNo);
        curPage->setNextPage(newPageNo);

        // Unpin the current page after linking it to the new page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;

        // Update header to reflect the new last page
        headerPage->lastPage = newPageNo;
        hdrDirtyFlag = true;

        // Set the current page to the new page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = true;

        // Continue the loop to retry inserting the record into the newly allocated page
    }
}
