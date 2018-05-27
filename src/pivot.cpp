/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2017 -*- C++ -*- */
/*
 * Description: User Defined Transform Function for pivot: for each partition, convert each different values of 1st parameter as columns, and sum 2nd parameter as value
 *
 * Create Date: Dec 16, 2017
 */

#include "Vertica.h"
#include <sstream>
#include <map>

using namespace Vertica;
using namespace std;

#define DEFAULT_separator ","

// methods for measureValues calcuated
#define methodSUM "SUM"
#define methodFIRST "FIRST"
#define methodLAST "LAST"
#define DEFAULT_method methodSUM

class Pivot : public TransformFunction
{
private:
    int measureCount;
    // note: use VerticaType** except VerticaType*, because there is no default constructor of class VerticaType for convenient initializing array. 
    VerticaType** measureTypePtrPtr;
	int columnsCount;
    std::map<std::string, int>* columnNames;
    std::string method;

    // buffer for SUM/FIRST/LAST operations: vint/vfloat/VNumeric* [measureCount][columnsCount]
    // note: use VNumeric* except VNumeric, because there is no default constructor of class VNumeric for convenient initializing array. 
    void** measurePtrPtr;

public:
    Pivot(): measureCount(0), measureTypePtrPtr(NULL), columnsCount(0), columnNames(NULL), method(DEFAULT_method), measurePtrPtr(NULL)
    {
    }

	virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &input_types)
    {
        // check arguments
        measureCount = input_types.getColumnCount() - 1;
        if ( measureCount < 1 ) 
            vt_report_error(0, "There should be 2 or more arguments, but [%zu] arguments are provided!", input_types.getColumnCount());

        VerticaType columnsFilterType = input_types.getColumnType(0);
        if ( ! columnsFilterType.isStringType() )
            vt_report_error(0, "The 1st argument should be string, but type[%s] is provided!", columnsFilterType.getTypeStr());

        measureTypePtrPtr = new VerticaType*[measureCount];
        for(int midx = 0; midx < measureCount; midx++)
        {
            measureTypePtrPtr[midx] = new VerticaType(VUnspecOID, 0);
            (*measureTypePtrPtr[midx]) = input_types.getColumnType(midx+1);
            if ( !(*measureTypePtrPtr[midx]).isInt() && !(*measureTypePtrPtr[midx]).isFloat() && !(*measureTypePtrPtr[midx]).isNumeric() ) 
            {
                for(; midx>=0; midx--)
                    delete measureTypePtrPtr[midx];

                delete[] measureTypePtrPtr;
                measureTypePtrPtr = NULL;
                vt_report_error(0, "The [%zu] argument should be int, float or numeric, but type[%s] is provided!", measureCount + 1, input_types.getColumnType(midx+1).getTypeStr());
            }
        }

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        if (paramReader.containsParameter("method"))
            method = paramReader.getStringRef("method").str();
            std::transform(method.begin(), method.end(), method.begin(), ::toupper);
        std::string separator = DEFAULT_separator;
        if (paramReader.containsParameter("separator"))
            separator = paramReader.getStringRef("separator").str();
        std::string columnsFilter = "";
        if (paramReader.containsParameter("columnsFilter"))
            columnsFilter = paramReader.getStringRef("columnsFilter").str();
        else 
            vt_report_error(0, "Function need at least parameter [columnsFilter]!");

        // split columnsFilter
        columnsCount = 0;
        istringstream ss(columnsFilter);
        const char delim = separator.c_str()[0];
        std::string token;
        while (getline(ss, token, delim))
            columnsCount++;

        columnNames = new std::map<std::string, int>;
        istringstream ss2(columnsFilter);
        int idx = 0;
        while (getline(ss2, token, delim))
            (*columnNames)[token] = idx++;

        // allocate buffer
        measurePtrPtr = new void*[measureCount];
        for(int midx = 0; midx < measureCount; midx++)
        {
            if ((*measureTypePtrPtr[midx]).isInt())
                measurePtrPtr[midx] = (void *) new vint[columnsCount];
            else if ((*measureTypePtrPtr[midx]).isFloat())
                measurePtrPtr[midx] = (void *) new vfloat[columnsCount];
            else if ((*measureTypePtrPtr[midx]).isNumeric())
            {
                int wordsCount = ((*measureTypePtrPtr[midx]).getNumericPrecision()+19)/19;
                uint64* wordsPtr = new uint64[wordsCount*columnsCount];
                measurePtrPtr[midx] = (void *) new VNumeric*[columnsCount];
                for(int idx = 0; idx < columnsCount; idx++)
                    ((VNumeric**)measurePtrPtr[midx])[idx] = new VNumeric(wordsPtr + wordsCount*idx, 
                                (*measureTypePtrPtr[midx]).getNumericPrecision(), (*measureTypePtrPtr[midx]).getNumericScale());
            }
        }
    }

	virtual void destroy (ServerInterface &srvInterface, const SizedColumnTypes &input_types){
        // free buffer
        if (measurePtrPtr != NULL) 
        {
            for(int midx = 0; midx < measureCount; midx++)
            {
                if ((*measureTypePtrPtr[midx]).isInt())
                    delete[] (vint*)measurePtrPtr[midx]; 
                else if ((*measureTypePtrPtr[midx]).isFloat())
                    delete[] (vfloat*)measurePtrPtr[midx]; 
                else if ((*measureTypePtrPtr[midx]).isNumeric())
                {
                    uint64* wordsPtr = ((VNumeric**)measurePtrPtr[midx])[0]->words;

                    for(int idx = 0; idx < columnsCount; idx++) 
                        delete ((VNumeric**)measurePtrPtr[midx])[idx];
                    delete[] (VNumeric**)measurePtrPtr[midx]; 

                    delete[] wordsPtr;
                }
            }

            measurePtrPtr = NULL;
        }

        if( columnNames != NULL ) 
        {
		    delete columnNames;
		    columnNames = NULL;
	    }

        if( measureTypePtrPtr != NULL )
        {
            for(int midx = 0; midx < measureCount; midx++)
                delete measureTypePtrPtr[midx];
            
            delete[] measureTypePtrPtr;
            measureTypePtrPtr = NULL;
        }
	}

    inline void processValueInt(bool &bColumnSet, vint &measure, const vint value)
    {
        if ( !bColumnSet || (method == methodLAST) ) 
        {
            measure = value;
            bColumnSet = true;
        }
        else if ( method == methodFIRST )
        {
            // skip
        } 
        else if ( (method == methodSUM) && (value != vint_null) ) 
        {
            if  ( measure == vint_null )
                measure = value;
            else
                measure += value;
        }
    }

    inline void processValueFloat(bool &bColumnSet, vfloat &measure, const vfloat value)
    {
        if (!bColumnSet || (method == methodLAST)) 
        {
            measure = value;
            bColumnSet = true;
        }
        else if ( method == methodFIRST )
        {
            // skip
        } 
        else if ( (method == methodSUM) && (!vfloatIsNull(value)) ) 
        {
            if ( vfloatIsNull(measure) )
                measure = value;
            else
                measure += value;
        }
    }

    inline void processValueNumeric(bool &bColumnSet, VNumeric &measure, const VNumeric *valuePtr)
    {
        if (!bColumnSet || (method == methodLAST)) 
        {
            measure.copy(valuePtr);
            bColumnSet = true;
        }
        else if ( method == methodFIRST )
        {
            // skip
        } 
        else if ( (method == methodSUM) && (!valuePtr->isNull()) ) 
        {
            if ( measure.isNull() )
                measure.copy(valuePtr);
            else 
                measure.accumulate(valuePtr);
        }
    }

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer)
    {
        if (input_reader.getNumCols() != measureCount + 1)
            vt_report_error(0, "Function need %zu arguments, but %zu provided", measureCount + 1, input_reader.getNumCols());
        // re-init buffer
        bool bColumnSet[measureCount][columnsCount];
        for(int midx = 0; midx < measureCount; midx++)
        {
            std::fill_n(bColumnSet[midx], columnsCount, false);
            if ((*measureTypePtrPtr[midx]).isInt())
                for(int idx = 0; idx < columnsCount; idx++) 
                    ((vint*)measurePtrPtr[midx])[idx] = vint_null;
            else if ((*measureTypePtrPtr[midx]).isFloat())
                for(int idx = 0; idx < columnsCount; idx++)
                    ((vfloat*)measurePtrPtr[midx])[idx] = vfloat_null;
            else if ((*measureTypePtrPtr[midx]).isNumeric())
                for(int idx = 0; idx < columnsCount; idx++)
                    ((VNumeric**)measurePtrPtr[midx])[idx]->setNull();
        }

        // SUM/FIRST/LAST operate on from 2nd parameter group by 1st parameter in each partition, considering NULL.
        do {
            // group by on 1st parameter 
            int idx;
            const VString& gby = input_reader.getStringRef(0);
            std::map<std::string, int>::iterator i = columnNames->find(gby.str());
            if( i != columnNames->end() )
                idx = i->second;
            else
                idx = columnsCount + 1;

            // SUM/FIRST/LAST operate on from 2nd parameter 
            if(idx >= 0 and idx < columnsCount) 
            {
                for(int midx = 0; midx < measureCount; midx++)
                {
                    if ((*measureTypePtrPtr[midx]).isInt()) 
                        processValueInt(bColumnSet[midx][idx], ((vint*)measurePtrPtr[midx])[idx], input_reader.getIntRef(midx + 1));
                    else if ((*measureTypePtrPtr[midx]).isFloat()) 
                        processValueFloat(bColumnSet[midx][idx], ((vfloat*)measurePtrPtr[midx])[idx], input_reader.getFloatRef(midx + 1));
                    else if ((*measureTypePtrPtr[midx]).isNumeric()) 
                        processValueNumeric(bColumnSet[midx][idx], *(((VNumeric**)measurePtrPtr[midx])[idx]), input_reader.getNumericPtr(midx + 1));
                }
            }

        } while (input_reader.next());

        // output
        for(int idx = 0; idx < columnsCount; idx++) 
        {
            for(int midx = 0; midx < measureCount; midx++)
            {
                if ((*measureTypePtrPtr[midx]).isInt()) 
                    output_writer.setInt(idx * measureCount + midx, ((vint*)measurePtrPtr[midx])[idx]);
                else if ((*measureTypePtrPtr[midx]).isFloat())
                    output_writer.setFloat(idx * measureCount + midx, ((vfloat*)measurePtrPtr[midx])[idx]);
                else if ((*measureTypePtrPtr[midx]).isNumeric()) 
                    output_writer.getNumericRef(idx * measureCount + midx).copy(((VNumeric**)measurePtrPtr[midx])[idx]);
            }
        }
        output_writer.next();
    }
};


class PivotFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &input_types, ColumnTypes &returnType)
    {
        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();

        input_types.addAny();
        
        // Note: need not add any type to returnType. empty returnType means any columnsFilter and types!
    }

    virtual void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &input_types, SizedColumnTypes &output_types)
    {
        // check arguments
        int measureCount = input_types.getColumnCount() - 1;
        if ( measureCount < 1 ) 
            vt_report_error(0, "There should be 2 or more arguments, but [%zu] arguments are provided!", input_types.getColumnCount());
        VerticaType columnsFilterType = input_types.getColumnType(0);
        if ( ! columnsFilterType.isStringType() )
            vt_report_error(0, "The 1st argument should be string, but type[%s] is provided!", columnsFilterType.getTypeStr());

        VerticaType** measureTypePtrPtr = new VerticaType*[measureCount];
        for(int midx = 0; midx < measureCount; midx++)
        {
            measureTypePtrPtr[midx] = new VerticaType(VUnspecOID, 0);
            (*measureTypePtrPtr[midx]) = input_types.getColumnType(midx+1);
            if ( !(*measureTypePtrPtr[midx]).isInt() && !(*measureTypePtrPtr[midx]).isFloat() && !(*measureTypePtrPtr[midx]).isNumeric() ) 
            {
                for(; midx>=0; midx--)
                    delete measureTypePtrPtr[midx];

                delete[] measureTypePtrPtr;
                measureTypePtrPtr = NULL;
                vt_report_error(0, "The [%zu] argument should be int, float or numeric, but type[%s] is provided!", measureCount + 1, input_types.getColumnType(midx+1).getTypeStr());
            }
        }

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        std::string method = DEFAULT_method;
        if (paramReader.containsParameter("method"))
            method = paramReader.getStringRef("method").str();
            std::transform(method.begin(), method.end(), method.begin(), ::toupper);
        std::string separator = DEFAULT_separator;
        if (paramReader.containsParameter("separator"))
            separator = paramReader.getStringRef("separator").str();
        std::string columnsFilter = "";
        if (paramReader.containsParameter("columnsFilter"))
            columnsFilter = paramReader.getStringRef("columnsFilter").str();
        else
            vt_report_error(0, "Function need at least parameter [columnsFilter]!");

        // output 
        istringstream ss(columnsFilter);
        const char delim = separator.c_str()[0];
        std::string token;
        while (getline(ss, token, delim)) 
        {
            for(int midx = 0; midx < measureCount; midx++)
            {
                std::stringstream columnName;
                columnName << token;
                if (midx > 0) 
                    columnName << "_" << midx;

                if ((*measureTypePtrPtr[midx]).isInt())
                    output_types.addInt(columnName.str());
                else if ((*measureTypePtrPtr[midx]).isFloat())
                    output_types.addFloat(columnName.str());
                else if ((*measureTypePtrPtr[midx]).isNumeric())
                    output_types.addNumeric((*measureTypePtrPtr[midx]).getNumericPrecision(), (*measureTypePtrPtr[midx]).getNumericScale(), columnName.str());
                else
                    vt_report_error(0, "Unkown type of 2 arguments: %s !", (*measureTypePtrPtr[midx]).getTypeStr());
            }
        }
    }

    // Defines the parameters for this UDSF. Works similarly to defining
    // arguments and return types.
    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes) 
    {
        parameterTypes.addVarchar(65000, "columnsFilter");
        //parameter: separator string for columnNames, default value is ','.
        parameterTypes.addVarchar(1, "separator");
        parameterTypes.addVarchar(10, "method");
    }


    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, Pivot); 
    }

};

RegisterFactory(PivotFactory);

