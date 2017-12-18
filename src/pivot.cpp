/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2017 -*- C++ -*- */
/*
 * Description: User Defined Transform Function for pivot: for each partition, convert each different values of 1st parameter as columns, and sum 2nd parameter as value
 *
 * Create Date: Dec 16, 2017
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;
using namespace std;

#define DEFAULT_separator ","


class Pivot : public TransformFunction
{
private:
    VerticaType measureType;
	int columnsCount;
    std::string* columnNames;

    // buffer for sum
    vint* measureIntPtr;
    vfloat* measureFloatPtr;
    uint64* measureNumericUInt64Ptr;
    VNumeric** measureNumericPtrPtr;

public:
    Pivot(): measureType(VUnspecOID, 0), columnsCount(0), columnNames(NULL),
             measureIntPtr(NULL), measureFloatPtr(NULL), measureNumericUInt64Ptr(NULL), measureNumericPtrPtr(NULL) 
    {
    }

	virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &input_types)
    {
        // check arguments
        if ( input_types.getColumnCount() != 2 ) 
            vt_report_error(0, "There should be 2 arguments, but [%zu] arguments are provided!", input_types.getColumnCount());

        VerticaType columnsFilterType = input_types.getColumnType(0);
        if ( ! columnsFilterType.isStringType() )
            vt_report_error(0, "The 1st argument should be string, but type[%s] is provided!", columnsFilterType.getTypeStr());

        measureType = input_types.getColumnType(1);
        if ( !measureType.isInt() && !measureType.isFloat() && !measureType.isNumeric() ) 
            vt_report_error(0, "The 1st argument should be int, float or numeric, but type[%s] is provided!", measureType.getTypeStr());

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
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

        columnNames = new std::string[columnsCount];
        istringstream ss2(columnsFilter);
        int idx = 0;
        while (getline(ss2, token, delim))
            columnNames[idx++] = token;

        // init buffer for sum
        if (measureType.isInt())
            measureIntPtr = new vint[columnsCount];
        else if (measureType.isFloat())
            measureFloatPtr = new vfloat[columnsCount];
        else if (measureType.isNumeric())
        {
            int wordsCount = (measureType.getNumericPrecision()+19)/19;
            measureNumericUInt64Ptr = new uint64[wordsCount*columnsCount];
            measureNumericPtrPtr = new VNumeric*[columnsCount];
            for(int idx = 0; idx < columnsCount; idx++)
                measureNumericPtrPtr[idx] = new VNumeric(measureNumericUInt64Ptr + wordsCount*idx, measureType.getNumericPrecision(), measureType.getNumericScale());
        }
    }

	virtual void destroy (ServerInterface &srvInterface, const SizedColumnTypes &input_types){
        if( columnNames != NULL ) 
        {
		    delete[] columnNames;
		    columnNames = NULL;
	    }

        // free buffer for sum
        if (measureIntPtr != NULL) 
        {
            delete[] measureIntPtr; 
            measureIntPtr = NULL;
        }
        if (measureFloatPtr != NULL) 
        {
            delete[] measureFloatPtr; 
            measureFloatPtr = NULL;
        }
        
        if (measureNumericPtrPtr != NULL) 
        {
            for(int idx = 0; idx < columnsCount; idx++) 
                delete measureNumericPtrPtr[idx];
            delete[] measureNumericPtrPtr; 
            measureNumericPtrPtr = NULL;
        }
        if (measureNumericUInt64Ptr != NULL) 
        {
            delete [] measureNumericUInt64Ptr;
            measureNumericUInt64Ptr = NULL;
        }
	}

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer)
    {
        if (input_reader.getNumCols() != 2)
            vt_report_error(0, "Function need 2 arguments, but %zu provided", input_reader.getNumCols());


        // re-init buffer for sum
        if (measureType.isInt())
            for(int idx = 0; idx < columnsCount; idx++) 
                measureIntPtr[idx] = vint_null;
        else if (measureType.isFloat())
            for(int idx = 0; idx < columnsCount; idx++)
                measureFloatPtr[idx] = vfloat_null;
        else if (measureType.isNumeric())
            for(int idx = 0; idx < columnsCount; idx++)
                measureNumericPtrPtr[idx]->setNull();

        // sum 2nd parameter group by 1st parameter in each partition, considering NULL.
        do {
            // group by on 1st parameter 
            const VString& gby = input_reader.getStringRef(0);
            int idx = 0;
            for(idx = 0; idx < columnsCount; idx++) 
                //srvInterface.log("DEBUG: %s.compare(%s)==%d", columnNames[idx].c_str(), gby.str().c_str(), columnNames[idx].compare(gby.str()));
                if( columnNames[idx].compare(gby.str()) == 0 )
                    break;

            // sum on 2nd parameter 
            if(idx >= 0 and idx < columnsCount) 
            {
                if (measureType.isInt()) 
                {
                    vint value = input_reader.getIntRef(1);
                    if ( value != vint_null ) 
                    {
                        if (measureIntPtr[idx] == vint_null )
                            measureIntPtr[idx] = value;
                        else
                            measureIntPtr[idx] += value;
                    }
                }
                else if (measureType.isFloat()) 
                {
                    vfloat value = input_reader.getFloatRef(1);
                    if ( !vfloatIsNull(value) ) 
                    {
                        if ( vfloatIsNull(measureFloatPtr[idx]) )
                            measureFloatPtr[idx] = value;
                        else
                            measureFloatPtr[idx] += value;
                    }
                }
                else if (measureType.isNumeric()) 
                {
                    const VNumeric* valuePtr = input_reader.getNumericPtr(1);
                    if ( !valuePtr->isNull() ) 
                    {
                        if ( measureNumericPtrPtr[idx]->isNull() )
                            measureNumericPtrPtr[idx]->copy(valuePtr);
                        else 
                            measureNumericPtrPtr[idx]->accumulate(valuePtr);
                    }
                }
            }

        } while (input_reader.next());


        // output
        for(int idx = 0; idx < columnsCount; idx++) 
        {
            if (measureType.isInt()) 
                output_writer.setInt(idx, measureIntPtr[idx]);
            else if (measureType.isFloat())
                output_writer.setFloat(idx, measureFloatPtr[idx]);
            else if (measureType.isNumeric()) 
                output_writer.getNumericRef(idx).copy(measureNumericPtrPtr[idx]);
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
        if ( input_types.getColumnCount() != 2 ) 
            vt_report_error(0, "There should be 2 arguments, but [%zu] arguments are provided!", input_types.getColumnCount());
        VerticaType columnsFilterType = input_types.getColumnType(0);
        if ( ! columnsFilterType.isStringType() )
            vt_report_error(0, "The 1st argument should be string, but type[%s] is provided!", columnsFilterType.getTypeStr());

        VerticaType measureType = input_types.getColumnType(1);
        if ( !measureType.isInt() && !measureType.isFloat() && !measureType.isNumeric() )
            vt_report_error(0, "The 1st argument should be int, float or numeric, but type[%s] is provided!", measureType.getTypeStr());

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
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
          if (measureType.isInt())
              output_types.addInt(token);
          else if (measureType.isFloat())
              output_types.addFloat(token);
          else if (measureType.isNumeric())
              output_types.addNumeric(measureType.getNumericPrecision(), measureType.getNumericScale(), token);
          else
              vt_report_error(0, "Unkown type of 2 arguments: %s !", measureType.getTypeStr());
        }
    }

    // Defines the parameters for this UDSF. Works similarly to defining
    // arguments and return types.
    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes) 
    {
        //parameter: separator string for columnNames, default value is ','.
        parameterTypes.addVarchar(255, "columnsFilter");
        parameterTypes.addVarchar(1, "separator");
    }


    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, Pivot); 
    }

};

RegisterFactory(PivotFactory);

