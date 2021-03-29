using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.Data;
using System.IO;

namespace updateDBwithCorrelativeInfo
{
    class Program
    {
        static int Main(string[] args)
        {
            string databasename = "";
            string CSVFile = "";
            string correlativeType = "";
            try
            {
                databasename = args[0];
                //string studyField = args[1];    // the name of the field which we want to add to the database
                CSVFile = args[1];
                correlativeType = args[2];
            }
            catch
            {
                Console.WriteLine("Please enter 3 valid parameters: Database path, CSV File Path, correlativeType");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            } 
             

            int parameterCount = args.Length;
            // Return an error if proper arguments are not entered
            if (parameterCount != 3)
            {
                Console.WriteLine("Please enter valid parameters.");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            } 

            int corrType_len = correlativeType.Length;

            if (!File.Exists(databasename) & !File.Exists(CSVFile))
            {
                Console.WriteLine("Database name and CSV file name entered are incorrect.");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            }
            else if (!File.Exists(CSVFile))
            {
                Console.WriteLine("CSV file name entered is incorrect.");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            }
            else if (!File.Exists(databasename))
            {
                Console.WriteLine("Database name entered is incorrect.");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            }

            // Establish connection with the database
            string connectionString = "Data Source = " + databasename + "; Version = 3; Synchronous = off; Cache Size = 16000; Default Timeout = 120";
            SQLiteConnection con = new SQLiteConnection(connectionString);

            // establish connection to the database
            try
            {
                con.Open();
            }
            catch
            {
                Console.WriteLine("Could not establish connection to database");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            }

            // Check if field CorrelativeData is part of Series table. If yes, continue 
            // Otherwise - Alter the table and add the field to the Series table.
            try
            {
                DataTable ColsTable = con.GetSchema("Columns");
                bool studyFieldExists = ColsTable.Select("COLUMN_NAME = 'CorrelativeData' AND TABLE_NAME = 'Series'").Length != 0;
                StringBuilder commandBuilder = new StringBuilder();
                SQLiteCommand cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                int rc;
                if (!studyFieldExists)
                {
                    commandBuilder = new StringBuilder();
                    commandBuilder.Append("ALTER TABLE Series ");
                    commandBuilder.Append("ADD COLUMN CorrelativeData TEXT");
                    cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                    rc = cmd.ExecuteNonQuery();
                }
                commandBuilder = new StringBuilder();
                commandBuilder.Append("UPDATE Series ");
                commandBuilder.Append("SET CorrelativeData = NULL ");
                cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                rc = cmd.ExecuteNonQuery();
            }
            catch
            {
                Console.WriteLine("Failed to add field 'CorrelativeData to Series table.");
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
                return 0;
            }
           

            try
            {
                Console.WriteLine("Updating Correlative Info...");
                // Load all the lines from CSV file
                string[] lines = LoadCSV(CSVFile);
                int num_rows = lines.Length;
                string corPatientName;
                string corPatientID;
                string cathDate;
                List<int> executions = new List<int>();
                for (int i = 1; i < num_rows; i++)
                {
                    string[] values = lines[i].Split(',');
                    corPatientName = values[0];
                    // Replace apostrophe (') in patient name and patient id with '?' to avoid use of SQLite syntax within the string
                    corPatientName = corPatientName.Replace("'", "?");
                    corPatientID = values[1];
                    corPatientID = corPatientID.Replace("'", "?");
                    cathDate = values[2];

                    // Convert the date to appropriate format and then express as 'yyyy-mm-dd'
                    //int m = cathDate.Length;
                    string cathDate2 = cathDate;
                    string date = "";
                    string year = "";
                    string month = "";
                    string day = "";
                    if (cathDate2[1] == '/' && cathDate2[3] == '/')
                    {
                        cathDate2 = cathDate2.Insert(0, "0");
                        cathDate2 = cathDate2.Insert(3, "0");
                    }
                    else if (cathDate2[1] == '/' && cathDate2[4] == '/')
                    {
                        cathDate2 = cathDate2.Insert(0, "0");

                    }
                    else if (cathDate2[2] == '/' && cathDate2[4] == '/')
                    {
                        cathDate2 = cathDate2.Insert(3, "0");
                    }

                    while (cathDate2.Length > 0)
                    {
                        if (cathDate2[0] != '/')
                        {
                            date = date + cathDate2[0];
                            cathDate2 = cathDate2.Remove(0, 1);
                        }
                        else
                        {
                            cathDate2 = cathDate2.Remove(0, 1);
                        }
                    }
                    month = date.Substring(0, 2);
                    day = date.Substring(2, 2);
                    year = date.Substring(4, 4);
                    string cathDate_new = year + "-" + month + "-" + day;
                    string cathDate_CorrData = month + "/" + day + "/" + year;

                    // SQLite query to check every entry in CSV file with the database and find the closest study date in the database
                    StringBuilder commandBuilder = new StringBuilder();
                    commandBuilder.Append("UPDATE Series ");
                    commandBuilder.Append(string.Format("SET CorrelativeData = '{0}' || ' (' || '{1}' || ')' ", correlativeType, cathDate_CorrData));
                    commandBuilder.Append(string.Format("WHERE (CAST (( abs( JulianDay('{0}') - JulianDay(substr(studydate, 1,4) || '-' || substr(studydate, 5, 2) || '-'  || substr(studydate,7,2)))) AS INT), PatientIndex)  IN ( ", cathDate_new));
                    commandBuilder.Append("SELECT MIN(diff) AS m, PatientIndex FROM ( ");
                    commandBuilder.Append(string.Format("SELECT CAST (( abs( JulianDay('{0}') - JulianDay(substr(studydate, 1,4) || '-' || substr(studydate, 5, 2) || '-'  || substr(studydate,7,2)))) AS INT) as diff, ser.PatientIndex, StudyDate, CorrelativeData ", cathDate_new));
                    commandBuilder.Append("FROM Series ser LEFT JOIN Patients pat ");
                    commandBuilder.Append("ON ser.PatientIndex = pat.PatientIndex ");
                    commandBuilder.Append(string.Format("WHERE RTRIM(replace(pat.patientname, '''','?'), '^') = '{0}' and replace(pat.patientid, '''','?') = '{1}' ", corPatientName, corPatientID));
                    commandBuilder.Append(string.Format("GROUP BY CAST((abs(JulianDay('{0}') - JulianDay(substr(studydate, 1, 4) || '-' || substr(studydate, 5, 2) || '-' || substr(studydate, 7, 2)))) AS INT) ", cathDate_new));
                    commandBuilder.Append(") AS aaa ");
                    commandBuilder.Append("GROUP BY PatientIndex ");
                    commandBuilder.Append(string.Format("HAVING CorrelativeData IS NULL OR m = min(m, CAST ((abs( JulianDay(substr(CorrelativeData,{0},4) || '-' || substr(CorrelativeData,{1},2) || '-'  || substr(CorrelativeData,{2},2)) - JulianDay(substr(studydate, 1,4) || '-' || substr(studydate, 5, 2) || '-'  || substr(studydate,7,2)))) AS INT)) )", corrType_len+9, corrType_len+3, corrType_len+6));                  
                    SQLiteCommand cmd = new SQLiteCommand(commandBuilder.ToString(), con);
                    int rc = cmd.ExecuteNonQuery();
                    executions.Add(rc);
                }
                bool allNonNegative = executions.All(x => x >= 0);
                if (allNonNegative)
                    Console.WriteLine("Correlative Data successfully updated in database.");
            }
            catch
            {
                Console.WriteLine("Failed to update Series table with correlative data. Ensure that the CSV file is closed before running the application.");
            }
            finally
            {
                // Wait for the user to respond before closing.
                Console.Write("Press any key to continue...");
                Console.ReadKey();
            }
            return 0;
        }

        // Function to load csv file
        private static string[] LoadCSV(string filename)
        {
            // Get the file's text.
            string whole_file = System.IO.File.ReadAllText(filename);

            // Split into lines.
            whole_file = whole_file.Replace('\n', '\r');
            string[] lines = whole_file.Split(new char[] { '\r' },
                StringSplitOptions.RemoveEmptyEntries);

            // Return the values.
            return lines;
        }
    }
}
