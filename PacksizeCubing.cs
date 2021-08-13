using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using Manh.WMFW.DataAccess;
using System.Data;
using Manh.WMFW.General;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System.Security.Principal;

using Manh.WMW.Printing.General;
using Manh.WMFW.Config.BL;
using System.Runtime.CompilerServices;

namespace BHS.PPlant.PacksizeCubing
{

    public class PacksizeCubing
    {

        private Manh.WMFW.General.Session _session;

        public void ExecuteStep(String stSV, int iWaveNum)
        {

            Manh.WMFW.General.Session session = SessionMapper.ConvertFromLegacySession(stSV);
            this._session = session;

            try
            {
                WriteDebug($"stSV {stSV} iWaveNum {iWaveNum}");

                ExecutePacksizeCubing(iWaveNum);

                GetDataTableFromCSVFile(iWaveNum);

            }
            catch (Exception ex)
            {
                ExceptionManager.LogException(session, ex, "PackSize-BHSCubing", iWaveNum, session.UserProfile.UserName);
                WriteDebug(string.Format("ExceptionCaught: {0} {1} {2}", ex.Message, Environment.NewLine, ex.StackTrace));
            }
        }

        private void ExecutePacksizeCubing(Int32 iWaveNum)
        {
            //int waveNumInt = Convert.ToInt32(iWaveNum);

            DataTable containerDataResults = GetItemDataForCubing(iWaveNum);


            WriteContainerDataFile(containerDataResults, iWaveNum.ToString());
        }

        private DataTable GetItemDataForCubing(int launchNum)
        {
            DataTable results = new DataTable();
            WriteDebug(string.Format("Launch Num {0}", launchNum));


            using (DataHelper dataHelper = new DataHelper((ISession)this._session))
            {
                IDataParameter[] parmarray = new IDataParameter[] {
                dataHelper.BuildParameter("@LAUNCH_NUM", launchNum)
                };

                results = dataHelper.GetTable(CommandType.StoredProcedure, "BHS_Packsize_GetItemDataForCubing", parmarray);
                WriteDebug(string.Format("Results {0}", results?.Rows?.Count ?? 0));

                return results;
            }

        }

        public void WriteContainerDataFile(DataTable containerDataResults, string launchNum)
        {

            if (!DataManager.IsEmpty(containerDataResults))
            {
                string _DirPath = GetPacksizeDirectory();
                string _FileExt = ".csv";

                WriteDebug(string.Format("_DirPath {0}, launchNum {1}, _FileExt {2}", _DirPath, launchNum, _FileExt));
                string _FilePath = _DirPath + launchNum + _FileExt;

                StringBuilder fileContent = new StringBuilder();

                foreach (DataRow dr in containerDataResults.Rows)
                {
                    foreach (var column in dr.ItemArray)
                    {
                        fileContent.Append(column.ToString() + ",");
                    }

                    fileContent.Replace(",", System.Environment.NewLine, fileContent.Length - 1, 1);
                }

                System.IO.File.WriteAllText(_FilePath, fileContent.ToString());
            }
        }

        private void WriteDebug(string text, [CallerMemberName] string member = "", [CallerLineNumber] int line = 0)
        {
            Debug.WriteLine(string.Format("{0} : {1} : {2} : {3}", this.GetType().FullName, member, line, text));
        }

        private string GetPacksizeDirectory()
        {
            string packsizeDirectory = SystemConfigRetrieval.GetStringSystemValue((ISession)this._session, "4020", "Technical");
            string path = FileManager.AddBackSlash(packsizeDirectory);

            WriteDebug("PacksizeDirectory : " + path);

            WriteDebug("Current User : " + WindowsIdentity.GetCurrent().Name);

            if (!Directory.Exists(path))
            {
                throw new Exception(String.Format("Packsize directory {0} does not exist.", path));
                //Directory.CreateDirectory(path);
            }

            return path;
        }

        private void GetDataTableFromCSVFile(int launchNum)
        {
            string packsizeDirectory = SystemConfigRetrieval.GetStringSystemValue((ISession)this._session, "4030", "Technical");
            string filePath = FileManager.AddBackSlash(packsizeDirectory);

            filePath = GetFilePath(filePath);

            WriteDebug("PacksizeDirectory 2: " + filePath);
            //string filePath = GetFilePath(@"\\precision-op\PacksizeCubeSolutions\");

            DataTable csvDataTable = new DataTable();

            try
            {
                using (TextFieldParser csvReader = new TextFieldParser(filePath))
                {
                    csvReader.SetDelimiters(new string[] { "," });
                    csvReader.HasFieldsEnclosedInQuotes = false;
                    string[] colFields = csvReader.ReadFields();
                    foreach (string column in colFields)
                    {
                        DataColumn datecolumn = new DataColumn(column);
                        datecolumn.AllowDBNull = true;
                        csvDataTable.Columns.Add(datecolumn);
                    }
                    while (!csvReader.EndOfData)
                    {
                        string[] fieldData = csvReader.ReadFields();
                        csvDataTable.Rows.Add(fieldData);
                    }
                }
            }
            catch (Exception ex)
            {
                WriteDebug(string.Format("Could not get data from CSV: {0}", ex));
            }

            try
            {
                InsertDataIntoSQL(csvDataTable);
            }
            catch (Exception ex)
            {
                WriteDebug(string.Format("Could not write data to database: {0}", ex));
            }

            try
            {


                using (DataHelper helper = new DataHelper())
                {
                    IDataParameter[] parmarray = new IDataParameter[] { helper.BuildParameter("@launchNum", launchNum) };

                    int num = helper.Update(CommandType.StoredProcedure, "BHS_ODWS_PacksizeShipAllo", parmarray);

                }
            }
            catch (Exception ex)
            {
                WriteDebug(string.Format("Could not update Shipment Allocation Request Table. Exception: " + ex.ToString()));
            }

            //move Packsize-produced file into Processed folder so that only one file remains in the base folder
            string newFilePath = filePath.Insert(37, @"Processed\");
            WriteDebug("NEW File path: " + newFilePath);
            System.IO.File.Move(filePath, newFilePath);


        }

        private void InsertDataIntoSQL(DataTable csvDataTable)
        {
            csvDataTable.Columns.Add("ERP_ORDER_LINE_NUM", typeof(decimal));

            foreach (DataColumn column in csvDataTable.Columns)
            {
                WriteDebug(column.ColumnName + column.DataType);
            }

            foreach (DataRow row in csvDataTable.Rows)
            {
                int charLocation = row["SKU"].ToString().IndexOf(":");

                string sku = row["SKU"].ToString().Substring(0, charLocation);
                decimal lineNum = Convert.ToDecimal(row["SKU"].ToString().Substring(charLocation + 1));

                WriteDebug("Index of : for this row - " + row["SKU"].ToString().IndexOf(":"));
                WriteDebug("SKU for this row: " + sku);
                WriteDebug("Line Num: " + lineNum);

                row["SKU"] = sku;
                row["ERP_ORDER_LINE_NUM"] = lineNum;

                WriteDebug(row["ERP_ORDER_LINE_NUM"].ToString());
            }

            using (DataHelper dataHelper = new DataHelper((ISession)this._session))
            {
                dataHelper.BulkWrite(csvDataTable, "BHS_PacksizeCubing", 1000);
            }
        }

        public string GetFilePath(string path)
        {

            for (int x = 0; x < 18; x++)
            {
                string[] file = new string[1];
                file = Directory.GetFiles(path);

                if (file.Length == 0)
                {
                    System.Threading.Thread.Sleep(10000);
                }
                else
                {
                    WriteDebug(file[0]);
                    return file[0];
                }
            }

            throw new Exception("Did not receive a file from Packsize");
        }
    }
}
