using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

public partial class Triggers
{
  /// <summary>
  /// GENERIC AUDIT TRIGGER: DataAudit.
  /// </summary>
  /// <remarks>
  /// This trigger class was created based on code originally sourced from
  /// http://sqljunkies.com/Article/4CD01686-5178-490C-A90A-5AEEF5E35915.scuk
  /// 
  /// This is the original template for Trigger metadata. Note that it is table-specific (i.e. it suggests
  /// that the trigger should apply to one table only).
  /// [Microsoft.SqlServer.Server.SqlTrigger(Name:="Trigger1", Target:="Table1", Event:="FOR UPDATE")] _
  ///
  /// This is our actual declaration. Note that it does not specify any particular table. We don't know
  /// if it is Microsoft's intention to allow table-agnostic trigger code, but this works and we hope
  /// that it keeps working.
  /// </remarks>
  [Microsoft.SqlServer.Server.SqlTrigger(Name="DataAudit", Event="FOR UPDATE, INSERT, DELETE")]
  public static void DataAudit()
  {

    try
    {

#if DEBUG
      EmitDebugMessage("Enter Trigger");
#endif

      // Grab the already-open Connection to use as an argument
#if DEBUG
      EmitDebugMessage("Open Connection");
#endif

      SqlTriggerContext context = SqlContext.TriggerContext; 
      SqlConnection connection = new SqlConnection("context connection=true"); 
      connection.Open();

      // Load the "inserted" table
#if DEBUG
      EmitDebugMessage("Load INSERTED");
#endif

      SqlDataAdapter tableLoader = new SqlDataAdapter("select * from inserted", connection); 
      DataTable insertedTable = new DataTable();
      tableLoader.Fill(insertedTable);

      // Load the "deleted" table
#if DEBUG
      EmitDebugMessage("Load DELETED");
#endif

      tableLoader.SelectCommand.CommandText = "select * from deleted"; 
      DataTable deletedTable = new DataTable(); 
      tableLoader.Fill(deletedTable);

      // Prepare the "audit" table for insertion
#if DEBUG
      EmitDebugMessage("Load AUDIT schema for insertion");
#endif

      SqlDataAdapter auditAdapter = new SqlDataAdapter("select * from audit where 1 = 0", connection); 
      DataTable auditTable = new DataTable(); 
      auditAdapter.FillSchema(auditTable, SchemaType.Source); 
      SqlCommandBuilder auditCommandBuilder = new SqlCommandBuilder(auditAdapter);

      // Create DataRow objects corresponding to the trigger table rows.
#if DEBUG
      EmitDebugMessage("Create internal representations of trigger table rows");
#endif

      string tableName = string.Empty; 
      DataRow insertedRow = null; 
      if (insertedTable.Rows.Count > 0) 
      { 
        insertedRow = insertedTable.Rows[0]; 
        tableName = DeriveTableNameFromKeyFieldName(insertedTable.Columns[0].ColumnName); 
      } 
      DataRow deletedRow = null; 
      if (deletedTable.Rows.Count > 0) 
      { 
        deletedRow = deletedTable.Rows[0]; 
        tableName = DeriveTableNameFromKeyFieldName(deletedTable.Columns[0].ColumnName); 
      }

      // get the current database user
      // TODO: cater for windows authentication "SELECT SYSTEM_USER"
      SqlCommand currentUserCmd = new SqlCommand("SELECT CURRENT_USER", connection); 
      string currentUser = currentUserCmd.ExecuteScalar().ToString();

            // Perform different audits based on the type of action.
            switch (context.TriggerAction)
            {
              case TriggerAction.Update:

                // Ensure that both INSERTED and DELETED are populated. If not, this is not a valid update.
                if ((insertedRow != null) && (deletedRow != null))
                {

                  // Walk through all the columns of the table.
                  foreach (DataColumn column in insertedTable.Columns)
                  {
                    // ALTERNATIVE CODE to compare values and record only if they are different:
                    // if (!DeletedRow.Item(Column.Ordinal).Equals(InsertedRow.Item(Column.Ordinal)))
                    
                    // This code records any attempt to update, whether the new value is different or not.
                    if (context.IsUpdatedColumn(column.Ordinal))
                    {
                      // DEBUG output indicating field change
#if DEBUG
                      EmitDebugMessage(string.Format("Create UPDATE Audit: Column Name = {0}, Old Value = '{1}', New Value = '{2}'", 
                                        column.ColumnName, 
                                        deletedRow[column.Ordinal].ToString(), 
                                        insertedRow[column.Ordinal].ToString()));
#endif

                      // Create audit record indicating field change
                      DataRow auditRow = auditTable.NewRow();

                      // populate fields common to all audit records
                      Int64 rowId = ((Int64)(insertedRow[0]));
                      // use "Inserted.TableName" when Microsoft fixes the CLR to supply it
                      WriteCommonAuditData(auditRow, tableName, rowId, currentUser, "UPDATE");
                      // write update-specific fields
                      auditRow["FieldName"] = column.ColumnName;
                      auditRow["OldValue"] = deletedRow[column.Ordinal].ToString();
                      auditRow["NewValue"] = insertedRow[column.Ordinal].ToString();

                      // insert the new row into the audit table
                      auditTable.Rows.InsertAt(auditRow, 0);
                    }
                  }
                }
                break;

              case TriggerAction.Insert:

                // If the INSERTED row is not populated, then this is not a valid insertion.
                if (insertedRow != null) 
                {

                  // DEBUG output indicating row insertion
#if DEBUG
                  EmitDebugMessage(string.Format("Create INSERT Audit: Row = '{0}'", insertedRow[0].ToString()));
#endif

                  // Create audit record indicating field change
                  DataRow auditRow = auditTable.NewRow();

                  // populate fields common to all audit records
                  Int64 rowId = ((Int64)(insertedRow[0]));
                  // use "Inserted.TableName" when Microsoft fixes the CLR to supply it
                  WriteCommonAuditData(auditRow, tableName, rowId, currentUser, "INSERT");

                  // insert the new row into the audit table
                  auditTable.Rows.InsertAt(auditRow, 0);

                }
                break;

              case TriggerAction.Delete:
                // If the DELETED row is not populated, then this is not a valid deletion.
                if (deletedRow != null)
                {
                  // DEBUG output indicating row insertion
#if DEBUG
                  EmitDebugMessage(string.Format("Create DELETE Audit: Row = '{0}'", deletedRow[0].ToString()) );
#endif

                  // Create audit record indicating field change
                  DataRow auditRow = auditTable.NewRow();

                  // populate fields common to all audit records
                  Int64 rowId = ((Int64)(deletedRow[0]));
                  // use "Inserted.TableName" when Microsoft fixes the CLR to supply it
                  WriteCommonAuditData(auditRow, tableName, rowId, currentUser, "DELETE");

                  // insert the new row into the audit table
                  auditTable.Rows.InsertAt(auditRow, 0);

                }
                break;

              default:
                break;
            }

            // update the audit table
            auditAdapter.Update(auditTable);

            // finish
#if DEBUG
            EmitDebugMessage("Exit Trigger");
#endif
        
  }
        catch (Exception e)
        {
          // Put exception handling code here if you want to connect this to your
          // database-based error logging system. Without this Try/Catch block,
          // any error in the trigger routine will stop the event that fired the trigger.
          // This is early-stage development and we're not expecting any exceptions,
          // so for the moment we just need to know about them if they occur.
	        throw;
        }


  }


  #region Helper Methods

  /// <summary>
  /// Write data into the fields of an Audit table row that is common to all types of audit activities.
  /// </summary>
  /// <param name="AuditRow">The audit row.</param>
  /// <param name="TableName">Name of the table.</param>
  /// <param name="RowId">The row id.</param>
  /// <param name="CurrentUser">The current user.</param>
  /// <param name="Operation">The operation.</param>
  private static void WriteCommonAuditData(DataRow auditRow, string tableName, Int64 rowId, string currentUser, string operation)
  {
    auditRow["TableName"] = tableName;
    auditRow["RowId"] = rowId;
    auditRow["OccurredAt"] = DateTime.Now;
    auditRow["PerformedBy"] = currentUser;
    auditRow["Operation"] = operation;
  }

  /// <summary>
  /// SQL CLR does not deliver the proper table name from either InsertedTable.TableName 
  /// or DeletedTable.TableName, so we must use a substitute based on our key naming
  /// convention. We assume that in each table, the KeyFieldName = TableName + "Id"
  /// Remove this routine and its uses as soon as we can get the table name from the CLR.
  /// </summary>
  /// <param name="KeyFieldName">Name of the key field.</param>
  /// <returns></returns>
  private static string DeriveTableNameFromKeyFieldName(string keyFieldName)
  {
    return (keyFieldName.Substring(0, keyFieldName.Length - 2)).ToUpper();
  }

#if DEBUG

  /// <summary>
  /// Emits the debug message.
  /// </summary>
  /// <param name="Message">The message.</param>
  private static void EmitDebugMessage(string message)
  {
    SqlContext.Pipe.Send(message);
  }

#endif

  #endregion

}
