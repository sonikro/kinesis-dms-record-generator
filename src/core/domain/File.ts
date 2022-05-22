import { JSONObject } from './JSONObject';

export type FileProps = {
  content: JSONObject | JSONObject[];
  order: number;
  schema: string;
  table: string;
  filename: string;
};

export default class File {
  private readonly fileParts: Array<string>;

  private readonly order: number;

  private readonly schema: string;

  private readonly table: string;

  private readonly filename: string;

  constructor(filename: string) {
    this.fileParts = this.validateFileParts(filename);
    const [order, schema, table] = this.fileParts;
    this.order = +order;
    this.schema = schema;
    this.table = table;
    this.filename = this.createFileName(`${this.order}`, this.schema, this.table);
  }

  getFileProperties(content: JSONObject | JSONObject[]): FileProps {
    return {
      content,
      order: this.order,
      schema: this.schema,
      table: this.table,
      filename: this.filename,
    };
  }

  createFileName(order: string, schema: string, table: string): string {
    return `${order}-${schema}-${table}`;
  }

  private validateFileParts(filename: string) {
    const fileParts = filename.split('.');
    if (fileParts.length < 4) {
      throw new Error(
        `Invalid file name ${filename}. Files should follow the pattern order.schema.table.json`,
      );
    }
    return fileParts;
  }
}
