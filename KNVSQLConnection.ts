import { invariant, precondition} from 'kneaver-stdjs/base';
import { SQLTag, KNVSQLCommand } from './KNVSQLCommand';

let isError = (e) => 
  ( e && 
    ((e instanceof Error) 
    || (e.stack && e.message && typeof e.stack === 'string' && typeof e.message === 'string'))
  )

let DBType = ( DB) => {
  if ( DB.query)
    return 'postgresql';
  else
  if ( DB.all)
    return 'sqlite';
  else
    return 'unknown';
};

function HackForGETItems( row)
{
  // Big _UGLY_ hack for aligning GEtItems with postgresql
  // Still necessary? Seems it's displaced in KNVSrv.OutItem
  if ( typeof row.links === "string")
  {
    row.links = JSON.parse(row.links);
  }
  if ( typeof row.names === "string")
  {
    row.names = JSON.parse(row.names);
  }
}

export class KNVSQLConnection
{
  DB: any;
  Tag: any;

  constructor( DB)
  {
    precondition( (typeof DB == "object") && (DB != null ))
    this.DB = DB;
    this.Tag = SQLTag( this);
  }
  // Private
  query( Cmd, Args, cb, def = undefined, failSilently = false)
  {
    let DB = this.DB;
    return new Promise( (resolve, reject) => {
      switch ( DBType( DB))
      {
        case 'postgresql': 
        {
          // console.log( "query", Cmd.Cmd, Args);
          DB.query( Cmd.Cmd
            , Args
            , (err, result) => {
                // console.log( "query", "cb", err, result);
                if ( err)
                {
                  if ( typeof def === 'undefined')
                  {
                    console.log( "Execute failed (1)", Cmd.Cmd, err, def);
                    reject( err);
                  }
                  else
                  if ( isError( def))
                  {
                    console.log( "Execute failed (2)", Cmd.Cmd, err, def);
                    reject( def);
                  }
                  else
                  {
                    let Cmd1 = Cmd.Cmd.trim();
                    let pos = Cmd1.indexOf( "RETURNING");
                    if ( pos != -1)
                    {
                      console.log( "Execute failed (3)", Cmd.Cmd, err, def);
                      reject( err);
                    }
                    const Cmd2 = Cmd1.substring(0, 7).toUpperCase();
                    if ( Cmd2 == "SELECT ")
                    {
                      if ( !failSilently)
                        console.log( "Select failed, ignored", Cmd.Cmd, err, def);
                      resolve( def);
                    }
                    else
                    {
                      if ( !failSilently)
                        console.log( "Execute failed, ignored", Cmd.Cmd, err, def);
                      resolve( def);
                    }
                  }
                }
                else {
                  // console.log( "Execute Succeeded", Cmd.Cmd, result);
                  let res = cb( result.rows, result.fields);
                  // console.log( "Execute Succeeded", Cmd.Cmd, result, res);
                  if ( isError( res))
                    reject( res);
                  else
                    resolve( res);
                }
          })
          break;
        }
        case 'sqlite':
        {
          let Cmd1 = Cmd.Cmd.trim();
          // console.log( "Cmd1", ">" + Cmd1 + "<");
          // console.log( "Args", Args);
          const Args1 = {};
          Args.forEach( (v, i) => {
            Args1[ `$${i+1}`] = v;
          });
          // console.log( "Args1", Args1);
          const Cmd2 = Cmd1.substring(0, 7).toUpperCase();
          // console.log( "Cmd2", ">" + Cmd2 + "<");
          if ( Cmd2 == "SELECT ")
          {
            // Args is {} invariant( Args.length === 0); 
            DB.all( Cmd1
                 , Args1
              , function(err, rows) {
                // console.log( "query.sqlite.all", Cmd1, err, rows);
                if ( err)
                {
                  if ( typeof def === 'undefined')
                  {
                    console.log( "Execute failed", Cmd1, err);
                    reject( err);
                  }
                  else
                  if ( isError( def))
                  {
                    console.log( "Execute failed", Cmd1, err);
                    reject( def);
                  }
                  else
                    resolve( def);
                }
                else 
                {
                  // console.log( "Execute Succeeded", Cmd, rows);
                  const fields = rows.length?Object.keys( rows[0]).map( name => ({name})):null;
                  // let rows2 = rows.map( (row) => Object.values( row));
                  invariant( typeof cb === "function");
                  const res = cb( rows, fields);

                  if ( isError( res))
                  {
                    console.log( "Execute failed", Cmd1, rows, res);
                    reject( res);
                  }
                  else
                    resolve( res);
                }
            })
        }
        else
        {
          let pos = Cmd1.indexOf( "RETURNING");
          if ( pos != -1)
          {
            Cmd1 = Cmd1.substring( 0, pos);
          }
          DB.run( Cmd1
            , Args1
            , function(err) {
              if ( err)
              {
                console.log( "Execute failed", Cmd1, err);
                if ( typeof def === 'undefined')
                  reject( err);
                else
                if ( isError( def))
                  reject( def);
                else
                  resolve( def);
              }
              else 
              {
                if ( Cmd2 == "INSERT ")
                {
                  let res = cb( [{ data: this.lastID}], [ { name: "data"}]);
                  if ( isError( res))
                    reject( res);
                  else
                    resolve( res);
                }
                else
                if (( Cmd2 == "UPDATE ") || ( Cmd2 == "DELETE "))
                {
                  let res = cb( [{ data: this.changes}], [ {name: "data"}]);
                  if ( isError( res))
                    reject( res);
                  else
                    resolve( res);
                }
                else
                if ( Cmd2 == "CREATE ")
                {
                  resolve( true);
                }
                else
                if ( Cmd2 == "ALTER T")
                {
                  resolve( true);
                }
                else
                {
                  console.log( "wrong command", Cmd2);
                  reject( false);
                }
              }
            })
          }
          break;
        }
      }
    });
  }

  // Public
  Execute( Cmd, Args = [])
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( true);
    return this.query( Cmd, Args, (rows) => (true));
  }
  LoggedExecute( Cmd, log, Args = [])
  {
    if ( Cmd.Cmd === '')
      return Promise.resolve( true);
    return this.query( Cmd, Args, (rows) => (true));
  }
  // GetStringValue
  // #TODO Add support for Args, similar to SQLQueryResultIsEmpty
  QueryHasRecords( Cmd, def = false, failSilently = false)
  {
    if ( Cmd.Cmd === '')
      return Promise.resolve( false);
    return this.query( Cmd, [], (rows) => (rows.length > 0), def, failSilently);
  }
  QueryFails( Cmd, Args = [])
  {
    if ( Cmd.Cmd === '')
      return Promise.resolve( false);
    return this.query( Cmd, Args, (rows) => (true), false, true);
  }
  SQLQueryResultIsEmpty( Cmd, Args = [])
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( false);
    return this.query( Cmd, Args, (rows) => (rows.length == 0));
  }
  SQLQueryResultCount( Cmd, Args = [])
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( false);
    return this.query( Cmd, Args, (rows) => (rows.length));
  }
  GetIntValue( Cmd, Args = [], def = undefined)
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( -1);
    return this.query( Cmd, Args, (rows, fields) => {
      if ( rows.length == 1)
      {
        let row = rows[0];
        let name = fields[ 0].name;
        // console.log( "GetIntValue", name, row);
        let val = row[ name ];
        // console.log( "GetInt", val);
        return parseInt( val);
      }
      else
      {
        // reject( "GetIntValue: no rows " + Cmd.Cmd);
        if ( def == undefined)
          return new Error( "GetStringValue: no rows " + Cmd.Cmd);

        return def;
      }
    }, def);
  }

  GetUUIDValue( Cmd, Args = [], def)
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( '00000000-0000-0000-0000-000000000000');
    return this.query( Cmd, Args, (rows, fields) => {
      if ( !fields)
      {
        if (def !== undefined)
          return def;
        // sqlite don't return fields if there are no rows
        if ( rows.length == 0)
          return new Error( "GetUUIDValue: no rows " + Cmd.Cmd);
        else
          return new Error( "GetUUIDValue: no fields " + Cmd.Cmd);
      }
      const name = fields[ 0].name;
      if ( rows.length == 1)
      {
        const row = rows[0];
        const val = row[ name ];
        return val;
      }
      else
      {
        if (def !== undefined)
          return def;
        return new Error( "GetUUIDValue: no rows " + Cmd.Cmd);
      }
    }, def);
  }
  GetStringValue( Cmd, Args = [], def)
  {
    if ( Cmd.Cmd == '')
    // TODO weird, either return def or raise exception'
        return Promise.resolve( '');
    return this.query( Cmd, Args, (rows, fields) => {
      if ( !fields)
      {
        // sqlite don't return fields if there are no rows
        if ( rows.length == 0)
          return new Error( "GetStringValue: no rows " + Cmd.Cmd);
        else
          return new Error( "GetStringValue: no fields " + Cmd.Cmd);
      }
      let name = fields[ 0].name;
      if ( rows.length == 1)
      {
        let row = rows[0];
        let val = row[ name ];
        return val;
      }
      else
      {
        return new Error( "GetStringValue: no rows " + Cmd.Cmd);
      }
    }, def);
  }
  // #TODO Add support for Args and def
  GetIntValues( Cmd)
  {
    if ( Cmd.Cmd == '')
        return Promise.resolve( -1);

    return this.query( Cmd.Cmd, [], (rows, fields) => {
      let name = fields[ 0].name;
      let res = [];
      for ( let i = 0; i < rows.length; i++)
      {
        let row = rows[ i];
        let val = row[ name ];
        res.push( parseInt( val));
      }
      return res;
    });
  }
  
  // query is private, Query is public
  Query( Cmd, Args = [])
  {
    if ( Cmd.Cmd == '')
    {
      console.log( "empty cmd", Cmd);
      return Promise.resolve( []);
    }

    return this.query( Cmd, Args, (rows, fields) => {
      // console.log( "rows", rows, fields);
      return rows;
    });
  }

  QueryOne( Cmd, Args = [])
  {
    if ( Cmd.Cmd == '')
    {
      console.log( "empty cmd", Cmd);
      return Promise.resolve( -1);
    }

    return this.query( Cmd, Args, (rows, fields) => {
      // console.log( "rows", rows, fields);
      if ( rows.length == 1)
      {
        HackForGETItems( rows[ 0]);
        return rows[0];
      }
      else
      {
        console.log( "error", rows);
        return new Error( "QueryOne: no rows or more than one " + Cmd.Cmd);
      }
    });
  }

  // #TODO Add support for Args and def
  SQLGetJSONData( Cmd, FieldName)
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( -1);
        
    return this.query( Cmd, [], (rows, fields) => {
      if ( rows.length == 1)
      {
        let name = fields[ 0].name;
        let row = rows[0];
        try {
          // BW 2018/11/17 Returning row[ data] directly if not a string + try/catch
          if ( typeof row[ name ] === "string")
          {
            let json = JSON.parse( row[ name ]);
            return json;
          }
          else
            return row[ name ];
        }
        catch ( error)
        {
          return new Error( "SQLGetJSONData: JSON can't parse " + Cmd.Cmd + " "+ name + " " + row[ name]);
        }
        return row;
      }
      else
      {
        return new Error( "SQLGetJSONData: no rows or more than one " + Cmd.Cmd);
      }
    });
  }
  // #TODO Add support for Args and def
  SQLGetArrayJSONData( Cmd, FieldName)
  {
    if ( Cmd.Cmd == '')
        return Promise.resolve( -1);
          
    return this.query( Cmd, [], (rows, fields) => {
      return rows.map( (row) => 
      {
        let name = fields[ 0].name;
        try {
          // BW 2018/11/17 Returning row[ data] directly if not a string + try/catch
          if ( typeof row[ name ] === "string")
          {
              let json = JSON.parse( row[ name ]);
              return json;
          }
          else
            return row[ name ];
        }
        catch ( error)
        {
          return new Error( "SQLGetJSONData: JSON can't parse " + Cmd.Cmd + " "+ name + " " + row[ name]);
        }
        return row;
      });
    });
  }
  // #TODO Add support for Args and def
  SQLGetArray( Cmd, FieldName)
  {
    if ( Cmd.Cmd == '')
      return Promise.resolve( -1);
        
    return this.query( Cmd, [], (rows, fields) => {
      return rows
    });
  }
  async MaxIDGetNewID( TableName)
  {
    switch ( DBType( this.DB))
    {
      case 'postgresql': 
      {
        // Server side allocation is like in old time based on the sequence
        let Cmd = SQLTag( this)`select nextval(${'"'+TableName+'_id_seq'+'"'}::regclass)`;
        let id = await this.GetIntValue( Cmd, []);  
        return id;
      }
      default: 
      {
        let Cmd = new KNVSQLCommand( "Select MaxID from MaxIDs where SequenceName = %1%");
        Cmd.SetDriver( this);
        Cmd.SetParamString( 1, TableName);
        const Rec = await this.Query( Cmd) as Array<any>;
        let res = 1;

        if (Rec.length)
        {
          res = Rec[0][ "MaxID" ];
          res++;
          let Cmd = new KNVSQLCommand( "Update MaxIDs set MaxID = %2% where SequenceName = %1%");
          Cmd.SetDriver( this);
          Cmd.SetParamString( 1, TableName);
          Cmd.SetParamInt( 2, res);
          await this.Execute( Cmd);
        }
        else
        {
          let Cmd = new KNVSQLCommand( "INSERT INTO MaxIDs ( SequenceName, MaxID) VALUES ( %1%, %2%)");
          Cmd.SetDriver( this);
          Cmd.SetParamString( 1, TableName);
          Cmd.SetParamInt( 2, res);
          await this.Execute( Cmd);
        }
        return res;
      };
    }
  }

  GetDialect()
  {
    return DBType( this.DB);
  }
}
