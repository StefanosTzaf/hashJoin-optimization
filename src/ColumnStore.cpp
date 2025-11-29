#include "ColumnStore.h"
#include "LateMaterialization.h"

Page* ColumnT::newPage(){

    Page* page = new Page;
 
    pages.push_back(page);
    return page;
}


size_t ColumnTInserter::dataBegin(Page* page){

    // first 2 bytes of the page are its number of values
    uint16_t num_of_values = *reinterpret_cast<uint16_t*>(page->data);

    // position for insertion is after metadata
	if(num_of_values == 0){
        return sizeof(num_of_values);
    }

    // go to the end of last value_t
    else{
        return sizeof(num_of_values) + sizeof(value_t)*num_of_values;
    }
}

bool ColumnTInserter::isFull(Page* page){
    
    size_t data = dataBegin(page);

    // if the space left is less the size of a single value_t
    // the page is full
    if((PAGE_SIZE - data) < sizeof(value_t)){
        return true;
    }

    else{
        return false;
    }
}

void ColumnTInserter::insert(value_t& val){
    
    // check if we need to create first page
    if(column.pages.size() == 0){
        column.newPage();
        
        // initialize page with numOfValues = 0
        *reinterpret_cast<uint16_t*>(column.pages[0]->data) = 0;
        
        // no need to increment lastPageIdx, it is already 0
    }
    
    // create and write to new page
    if(isFull(column.pages[lastPageIdx])){
        lastPageIdx++;
        column.newPage();
        
        // get new page
        Page* page = column.pages[lastPageIdx];
        
        // initialize page with numOfValues = 0
        *reinterpret_cast<uint16_t*>(page->data) = 0;
    }

    Page* page = column.pages[lastPageIdx];

    // calculate position to insert new value_t
    size_t last_data = dataBegin(page);

    // write value_t 
    *reinterpret_cast<value_t*>(page->data + last_data) = val;

    // increment numOfValues metadata
    uint16_t* numOfValues = reinterpret_cast<uint16_t*>(page->data);
    (*numOfValues)++;

    column.size++;
}