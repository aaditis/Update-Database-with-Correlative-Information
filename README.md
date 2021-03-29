# Update-Database-with-Correlative-Information

The console app involved loading and parsing a CSV file. The CSV file contained attributes like PatientName, PatientID and CorrelativeDate. The functionality was to read the
PatientName, PatientID, and CorrelativeDate from the CSV file and search the database to find a patient with the same PatientName and PatientID and a StudyDate that is the closest chronologically to corStudyDate from the CSV file (since a single patient is associated with multiple studies and hence multiple study dates). The database was updated when a match was found.
