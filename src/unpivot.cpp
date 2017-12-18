/* Copyright (c) DingQiang Liu(dingqiangliu@gmail.com), 2012 - 2017 -*- C++ -*- */
/*
 * Description: User Defined Transform Function for UnPivot: convert columns to rows
 * 
 * Create Date: Dec 17, 2017
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;
using namespace std;

#define DEFAULT_separator ","


class UnPivot : public TransformFunction
{
private:
    VerticaType measureType;
	int measureNamesCount;
    std::string* columnNames;

public:
    UnPivot(): measureType(VUnspecOID, 0), measureNamesCount(0), columnNames(NULL)
    {
    }

	virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &input_types)
    {
        // check arguments
        measureNamesCount = input_types.getColumnCount();
        if ( measureNamesCount  < 1 ) 
            vt_report_error(0, "There should be more than 1 arguments, but [%s] arguments are provided!", input_types.getColumnCount());

        measureType = input_types.getColumnType(0);
        for(int idx = 1; idx < measureNamesCount; idx++) 
        {
            VerticaType measureTypeOther = input_types.getColumnType(1);
            if ( measureTypeOther != measureType )
                vt_report_error(0, "All type of arguments should same!");
        }

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        std::string separator = DEFAULT_separator;
        if (paramReader.containsParameter("separator"))
            separator = paramReader.getStringRef("separator").str();
        std::string measureNames = "";
        if (paramReader.containsParameter("measureNames"))
            measureNames = paramReader.getStringRef("measureNames").str();
        else 
            vt_report_error(0, "Function need at least parameter [measureNames]!");

        // split measureNames
        const char delim = separator.c_str()[0];
        std::string token;
        int measureNamesCountFromParameter = 0;
        istringstream ss(measureNames);
        while (getline(ss, token, delim))
            measureNamesCountFromParameter++;

        if(measureNamesCountFromParameter == measureNamesCount)
        {
            columnNames = new std::string[measureNamesCount];
            istringstream ss2(measureNames);
            int idx = 0;
            while (getline(ss2, token, delim))
                columnNames[idx++] = token;
        }
        else
        {
            // TODO: getColumnName if there is no measureNames paramter
        }
    }

	virtual void destroy (ServerInterface &srvInterface, const SizedColumnTypes &input_types)
    {
        if( columnNames != NULL ) 
        {
		    delete[] columnNames;
		    columnNames = NULL;
	    }
	}

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &input_reader,
                                  PartitionWriter &output_writer)
    {
        // check arguments
        if ((int)input_reader.getNumCols() != measureNamesCount)
            vt_report_error(0, "There should be [%zu] arguments, but [%zu] arguments are provided!", measureNamesCount, input_reader.getNumCols());

        // output: convert columns to rows
        do 
        {
            for(int idx = 0; idx < measureNamesCount; idx++) 
            {
                output_writer.getStringRef(0).copy(columnNames[idx].c_str());

                if (measureType.isInt()) 
                {
                    output_writer.setInt(1, input_reader.getIntRef(idx));
                }
                else if (measureType.isFloat()) 
                {
                    output_writer.setFloat(1, input_reader.getFloatRef(idx));
                }
                else if (measureType.isNumeric()) 
                {
                    output_writer.getNumericRef(1).copy(input_reader.getNumericPtr(idx));
                }

                output_writer.next();
            }
        } while (input_reader.next());
    }
};


class UnPivotFactory : public TransformFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &input_types, ColumnTypes &returnType)
    {
        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();

        input_types.addAny();
        
        // Note: need not add any type to returnType. empty returnType means any measureNames and types!
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &input_types,
                               SizedColumnTypes &output_types)
    {
        // check arguments
        int measureNamesCount = input_types.getColumnCount();
        if ( measureNamesCount  < 1 ) 
            vt_report_error(0, "There should be more than 1 arguments, but [%zu] arguments are provided!", input_types.getColumnCount());

        VerticaType measureType = input_types.getColumnType(0);
        for(int idx = 1; idx < measureNamesCount; idx++) 
        {
            VerticaType measureTypeOther = input_types.getColumnType(1);
            if ( measureTypeOther != measureType ) 
                vt_report_error(0, "All type of arguments should same!");
        }

        // get parameters
        ParamReader paramReader = srvInterface.getParamReader();
        std::string separator = DEFAULT_separator;
        if (paramReader.containsParameter("separator"))
            separator = paramReader.getStringRef("separator").str();
        std::string measureNames = "";
        if (paramReader.containsParameter("measureNames"))
            measureNames = paramReader.getStringRef("measureNames").str();
        else 
            vt_report_error(0, "Function need at least parameter [measureNames]!");

        unsigned int length = 1;
        istringstream ss(measureNames);
        const char delim = separator.c_str()[0];
        std::string token;
        while (getline(ss, token, delim))
            length = (token.length() > length)? token.length(): length;

        // output 
        output_types.addVarchar(length, "measureName");
        if (measureType.isInt()) 
            output_types.addInt("meaureValue");
        else if (measureType.isFloat()) 
            output_types.addFloat("meaureValue");
        else if (measureType.isNumeric()) 
            output_types.addNumeric(measureType.getNumericPrecision(), measureType.getNumericScale(), "meaureValue");
        else
            vt_report_error(0, "Unkown type of 2 arguments: %s !", measureType.getTypeStr());
    }

    // Defines the parameters for this UDSF. Works similarly to defining
    // arguments and return types.
    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes) 
    {
        //parameter: separator string for columnNames, default value is ','.
        parameterTypes.addVarchar(255, "measureNames");
        parameterTypes.addVarchar(1, "separator");
    }


    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObj(srvInterface.allocator, UnPivot); 
    }

};

RegisterFactory(UnPivotFactory);

