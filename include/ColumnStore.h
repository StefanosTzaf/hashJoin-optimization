#pragma once
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "LateMaterialization.h"



class ColumnT{
    private:
        DataType type; // type of column
        size_t size; // number of elements (+null)
        std::vector<Page*> pages;

    public:
        // constructor
        ColumnT(DataType typeCol): type(typeCol), pages(), size(0){}; 

        // efficient move of column: && rvalue
        ColumnT(ColumnT&& col): type(col.type), pages(std::move(col.pages)){}
        
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
        size_t dataBegin(Page* page);

        // returns true if the page is full
        bool isFull(Page* page);
        
        // inserts a value_t in the last page or a new one if there is no space
        void insert(value_t& val);

        // GETTERS
        const int getLastPage() const{
            return lastPageIdx;
        }

        const ColumnT& getColumn()const{
            return column;
        }

};
