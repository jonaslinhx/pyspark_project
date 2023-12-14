from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    TimestampType,
    NullType,
    ArrayType
)

account_schema = StructType([
    StructField("load_date", DateType()),
    StructField("active_ind", IntegerType()),
    StructField("account_id", StringType()),
    StructField("source_sys", StringType()),
    StructField("account_start_date", TimestampType()),
    StructField("legal_title_1", StringType()),
    StructField("legal_title_2", StringType()),
    StructField("tax_id_type", StringType()),
    StructField("tax_id", StringType()),
    StructField("branch_code", StringType()),
    StructField("country", StringType())
])
party_schema = StructType([
    StructField("load_date", DateType()),
    StructField("account_id", StringType()),
    StructField("party_id", StringType()),
    StructField("relation_type", StringType()),
    StructField("relation_start_date", TimestampType())
])

address_schema = StructType([
    StructField("load_date", StringType()),
    StructField("party_id", IntegerType()),
    StructField("address_line_1", StringType()),
    StructField("address_line_2", StringType()),
    StructField("city", StringType()),
    StructField("postal_code", StringType()),
    StructField("country_of_address", StringType()),
    StructField("address_start_date", DateType())
])

contract_schema = StructType([
    StructField("account_id", StringType()),
    StructField("contractIdentifier", 
                StructType([StructField("operation", StringType()),
                            StructField("newValue", StringType()),
                            StructField("oldValue", NullType())])),
    StructField("sourceSystemIdentifier", 
                StructType([StructField("operation", StringType()),
                            StructField("newValue", StringType()),
                            StructField("oldValue", NullType())])),
    StructField("contractStartDateTime",
                StructType([StructField("operation", StringType()),
                            StructField("newValue", StringType()),
                            StructField("oldValue", NullType())])),
    StructField("contractTitle",
                StructType([StructField("operation", StringType()),
                            StructField("newValue", 
                                ArrayType(StructType([
                                    StructField("contractTitleLineType", StringType()),
                                    StructField("contractTitleLine", StringType())
                                ]))
                            ),
                            StructField("oldValue", NullType())])),
    StructField("taxIdentifier",
                StructType([StructField("operation", StringType()),
                            StructField("newValue",
                                StructType([
                                    StructField("taxIdType", StringType()),
                                    StructField("taxId", StringType())
                                ])),
                            StructField("oldValue", NullType())])),
    StructField("contractBranchCode", StringType()),
    StructField("contractCountry", StringType())
])