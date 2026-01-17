#pragma once
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include <iostream>
#include "value_t.h"

struct value_t;


class ColumnT{
    private:
        DataType type; // type of column
        size_t size; // number of elements (+null)
        std::vector<Page*> pages;
        std::vector<size_t> pageRowOffset; // a vector holding the number of offset rows till each page

        bool copied; // if copied = true, column is not dense
        const Column* colRef; // if copied = false, colRef points to the original Column

    public:
        // constructor
        ColumnT(DataType typeCol): type(typeCol), pages(), size(0), pageRowOffset(), copied(true), colRef(NULL){}; 

        // construct ColumnT from Column for dense columns
        ColumnT(const Column* col): colRef(col), copied(false), pageRowOffset(), type(col->type){
           
            size_t totalRows = 0;
            size_t idx = 0;
            for(Page* page: colRef->pages){
                uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);
                
                pageRowOffset.push_back(totalRows); // number of rows till the beginning of page idx
                idx++;

                totalRows += numRows;
            }

            size = totalRows;
        };

        // creates a new page, allocates memory for it and adds it to the vector
        Page* newPage();

        // adds existing page to the vector
        void addPage(Page* page);

        bool isCopied() const{
            return copied;
        }

        // GETTERS
        const size_t& getSize() const{
            return size;
        }

        const DataType& getType() const{
            return type;
        }

        const int getNumPages() const{

            if(copied == false){
                return colRef->pages.size();
            }

            return pages.size();
        } 

        Page* getPage(size_t idx) const{

            if(copied == false){
                return colRef->pages[idx];
            }

            return pages[idx];
        }

        const std::vector<Page*>& getPages() const{

            if(copied == false){
                return colRef->pages;
            }

            return pages;
        }

        const std::vector<size_t>& getPageRowOffsets() const{

            return pageRowOffset;
        }

        // returns a pointer to the value_t in idx row
        inline value_t* getValueAt(size_t rowIdx) const{
            
            if(rowIdx >= size){
                return NULL;
            }

            // first element that is greater than idx
            auto it = std::upper_bound(pageRowOffset.begin(), pageRowOffset.end(), rowIdx);

            // find previous page index
            // (it - pageRowOffset.begin) = index it
            size_t pageIdx = (it - pageRowOffset.begin()) - 1; 

            size_t pageStart = pageRowOffset[pageIdx];
            size_t dataIdx = rowIdx - pageStart; //row inside page

            value_t* val = reinterpret_cast<value_t*>(pages[pageIdx]->data 
                        + sizeof(uint16_t) + dataIdx*sizeof(value_t));

            return val;
        }

        inline int32_t* getValueAtRef(size_t rowIdx) const{
            
            if(rowIdx >= size){
                return NULL;
            }

            // first element that is greater than idx
            auto it = std::upper_bound(pageRowOffset.begin(), pageRowOffset.end(), rowIdx);

            // find previous page index
            // (it - pageRowOffset.begin) = index it
            size_t pageIdx = (it - pageRowOffset.begin()) - 1; 

            size_t pageStart = pageRowOffset[pageIdx];
            size_t dataIdx = rowIdx - pageStart; //row inside page

            // find value inside original column through colRef
            int32_t* val = reinterpret_cast<int32_t*>(colRef->pages[pageIdx]->data + 2*sizeof(uint16_t)
                        + dataIdx*sizeof(int32_t));

            return val;
        }

        inline void* getValueAtRow(size_t row_idx) const{
            if(copied == true){
                value_t* val = getValueAt(row_idx);
                return val;
            }
            else{
                int32_t* val = getValueAtRef(row_idx);
                return val;
            }
        }

        // destructor
        ~ColumnT(){
            if(copied == true){

                for(Page* page: pages){
                    delete(page);
                }
            }
        }

        // Move constructor: create ColumnT from existing one
        ColumnT(ColumnT&& other): type(other.type), size(other.size), 
            pages(std::move(other.pages)), pageRowOffset(std::move(other.pageRowOffset)), 
            copied(other.copied), colRef(other.colRef) {

                other.pages.clear();
                other.pageRowOffset.clear();
        }

        // Move assignment: used in my_copy_scan to assign a Column to an already existing
        // ColumnT that was constructed as a copied column 
        ColumnT& operator=(ColumnT&& other){
            
            if(this != &other){

                // deallocate memory only for copied pages
                if(copied == true){
                    for(Page* page : pages) {
                        delete page;
                    }
                }
                type = other.type;
                size = other.size;
                pages = std::move(other.pages);
                pageRowOffset = std::move(other.pageRowOffset);
                copied = other.copied;
                colRef = other.colRef;

            }
            return *this;
        }


        friend class ColumnTInserter;
        
};

// used for inserting value_t to a given column
class ColumnTInserter{
    private:
        ColumnT& column;
        int lastPageIdx = 0;

    public:
        // constructor 
        ColumnTInserter(ColumnT& newCol): column(newCol){
            if(column.pages.size() == 0){
                lastPageIdx = 0;
            }
            else{
                lastPageIdx = column.pages.size();
            }
        }

        // returns the position for the next insertion
        size_t dataBegin(Page* page) const;

        // returns true if the page is full
        bool isFull(Page* page) const;
        
        // inserts a value_t in the last page or a new one if there is no space
        void insert(const value_t& val);

        // inserts a whole page in the column
        void insertPage(Page* page);

        // GETTERS
        const int getLastPageIdx() const{
            return lastPageIdx;
        }

        const ColumnT& getColumn()const{
            return column;
        }



};
