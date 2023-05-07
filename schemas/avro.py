schemaDepartments = {
    'doc': 'departments',
    'name': 'departments',
    'namespace': 'departments',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
    {'name': 'department', 'type': 'string'},
    ]
}

schemaJob = {
    'doc': 'job',
    'name': 'job',
    'namespace': 'job',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'job', 'type': 'string'},
    ]
}

schemaHiredEmployees = {
    'doc': 'hired_employees',
    'name': 'hired_employees',
    'namespace': 'hired_employees',
    'type': 'record',
    'fields': [
        {'name': 'id', 'type': 'int'},
        {'name': 'name', 'type': 'string'},
        {'name': 'datetime', 'type': 'string'},
        {'name': 'department_id', 'type': 'int'},
        {'name': 'job_id', 'type': 'int'},
    ]
}
class AvroSchemas:

    def getSchema(tableName):
        if (tableName=="departments"):
            return schemaDepartments
        elif (tableName=="jobs"):
             return schemaJob
        elif (tableName=="hired_employees"):
             return schemaHiredEmployees