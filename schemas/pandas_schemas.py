from pandas_schema import Column, Schema
from pandas_schema.validation import CustomElementValidation,LeadingWhitespaceValidation, TrailingWhitespaceValidation, CanConvertValidation, MatchesPatternValidation, InRangeValidation, InListValidation
import pandas as pd 
import re

def check_int(num):
    try:
        int(num)
    except ValueError:
        return False
    return True

def check_str(strin):
    try:
        isinstance(strin, str)
    except ValueError:
        return False
    value= True if (not strin=="NULL") else False
    return value


def check_date(strin):
        x = re.search("\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z)", str(strin))
        return (True if x else False)
    
class PandasSchemas:
    
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'Is not a integer number')]
    str_validation = [CustomElementValidation(lambda i: check_str(i), 'Is not a string')]
    date_validation = [CustomElementValidation(lambda i: check_date(i), 'Wrong datetime format')]

    schema_departments = Schema([
        Column('id', int_validation),
        Column('department',str_validation),
    ], False)

    schema_jobs = Schema([
        Column('id', int_validation),
        Column('job',str_validation),
    ],False)

    schema_hired_employees = Schema([
        Column('id', int_validation),
        Column('name'),
        Column('datetime',date_validation),
        Column('department_id', int_validation),
        Column('job_id', int_validation),
    ],False)

    def validateSchemas(schema:Schema,df:pd.DataFrame):
        validation=schema.validate(df)
        return validation