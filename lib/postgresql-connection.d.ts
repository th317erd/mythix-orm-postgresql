import { SQLConnectionBase } from "mythix-orm-sql-base";

declare class PostgreSQLConnection extends SQLConnectionBase {
  public formatResultsResponse(sqlStatement: string, result: any): any;
}

export default PostgreSQLConnection;
