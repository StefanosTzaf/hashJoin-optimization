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

    public:
        // constructor
        ColumnT(DataType typeCol): type(typeCol), pages(), size(0), pageRowOffset(){}; 

        // efficient move from Column NOT ColumnT
        ColumnT(Column&& col): type(col.type), pages(std::move(col.pages)){

            int sum = 0;
            pageRowOffset.reserve(pages.size());

            switch (col.type){
                case DataType::INT32:

                    for(Page* page: pages){

                        uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);
                        pageRowOffset.push_back(sum);
                        sum += numRows;
                    }
                    break;

                case DataType::VARCHAR:
                    for(Page* page: pages){
                        
                        uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);
                        if((numRows == 0xffff)|| (numRows == 0xfffe)){
                            numRows = 0;
                        }
                        
                        pageRowOffset.push_back(sum);
                        sum += numRows;
                    }
                
                    break;
            }
            size = sum;
        }
        
        // creates a new page, allocates memory for it and adds it to the vector
        Page* newPage();

        // GETTERS
        const size_t& getSize() const{
            return size;
        }

        const DataType& getType() const{
            return type;
        }

        const int getNumPages() const{
            return pages.size();
        } 

        Page* getPage(size_t idx) const{
            return pages[idx];
        }

        const std::vector<Page*>& getPages() const{
            return pages;
        }

        // returns a pointer to the value_t in idx row
        value_t* getValueAt(size_t idx) const;


        // destructor
        ~ColumnT(){
            for(Page* page: pages){
                delete(page);
            }
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

        // GETTERS
        const int getLastPageIdx() const{
            return lastPageIdx;
        }

        const ColumnT& getColumn()const{
            return column;
        }



};
