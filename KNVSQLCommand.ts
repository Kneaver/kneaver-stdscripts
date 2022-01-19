const { invariant, precondition} = require( 'kneaver-stdjs/base');

export function SQLProtectName( Dialect, Name)
{
    return "\"" + Name + "\"";
}

export function SQLProtectString( Dialect, Value)
{
  // !!  since postgressql 9.1 behavior changed
  // standard_conforming_strings (boolean)
  // This controls whether ordinary string literals ('...') treat backslashes literally, as specified in the SQL standard. Beginning in PostgreSQL 9.1, the default is on (prior releases defaulted to off). Applications can check this parameter to determine how string literals will be processed. The presence of this parameter can also be taken as an indication that the escape string syntax (E'...') is supported. Escape string syntax (Section 4.1.2.2) should be used if an application desires backslashes to be treated as escape characters.

  invariant( typeof Value === "string");
  if ( Dialect === "sqlite")
  {
    return "\'" + 
      Value
      .replace( /'/g, "''")
      .replace( /\\/g, "\\\\")
      .replace( /"/g, "\"")  // protect ', ", and \
      .replace( /\\\\"/g, "\\\"")  // deprotect " that were already protected
      + "\'";
  }
  else
  {
    const res = "\'" + 
      Value
      .replace( /'/g, "''")
      .replace( /\$$/, "USDSign")
      .replace( /\?$/, "QuestionMarkSign")
      + "\'";
    return res;
  }
}

function SQLAdapt( Dialect, Name)
{
  if ( Dialect === "sqlite")
  {
    switch (Name.toLowerCase()) {
      case "long": return "integer";
      case "double": return "double precision";
      // case "datetime": return "INTEGER(4)";
      default: return Name;
    }
  }
  else
  {
    switch (Name.toLowerCase()) {
      case "long": return "integer";
      case "double": return "double precision";
      case "datetime": return "timestamp without time zone";
      default: return Name;
    }
  }
}

function SQLStripIndent(str) {
    // from: https://github.com/sindresorhus/strip-indent
    var match = str.match(/^[ \t]*(?=\S)/gm);

    if (!match) {
        return str;
    }

    var indent = Math.min.apply(Math, match.map(function (el) {
        return el.length;
    }));

    var re = new RegExp('^[ \\t]{' + indent + '}', 'gm');

    return indent > 0 ? str.replace(re, '') : str;
};

function Transform( Dialect, Cmd)
{
  if ( Dialect === "sqlite")
  {
    Cmd = Cmd.replace( "Now()", "(CURRENT_TIME)");
    Cmd = Cmd.replace( "NOW()", "(CURRENT_TIME)");
    Cmd = Cmd.replace( /ToJSDate\(([ A-Za-z0-9\"\.]+)\)/gm , "strftime('%Y-%m-%dT%H:%M:%SZ', $1)");
    Cmd = Cmd.replace( /\) l/gm , ")");
  }
  else
  {
    Cmd = Cmd.replace( /ToJSDate\(([ A-Za-z0-9\"\.]+)\)/gm , "to_char( $1, 'YYYY-MM-DD\"T\"HH:MI:SSZ')");
    Cmd = Cmd.replace( /json_group_array\( json_object\(([ A-Za-z0-9\',\n]+)\)\)/gm, "array_to_json( array_agg(l), true)");
  }
  return Cmd;
  /*
  modules.exports.GetItem2 = `
  ( SELECT array_to_json( array_agg(l), true) as data
    ) l
  `;
  */

  // SQLAdapt #TODO should be called ? No it's only for args
}

export class KNVSQLCommand
{
  Cmd: string;
  DB: any;

  constructor( Cmd)
  {
    this.Cmd = Cmd;
  }
  getDialect()
  { 
    return this.DB.GetDialect();
  }
  SetDriver( DB)
  {
    this.DB = DB;
    this.Cmd = Transform( this.getDialect(), this.Cmd);
    return this; // for sugar syntax
  }
  SetParamRaw( pos, Value)
  {
    invariant( typeof Value === "string");
    if ( this.DB)
      Value = Transform( this.DB.GetDialect(), Value);
    this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", Value);
    return this;
  }
  SetParamString( pos, Value)
  {
    invariant( typeof Value === "string");
    this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), Value));
    return this;
  }
  SetParamNull( pos, Value)
  {
      if ( Value === null)
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
      else
      if ( typeof Value === "undefined")
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
      else
      if ( typeof Value === "object")
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), JSON.stringify( Value)));
      else
      if ( typeof Value === "number")
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), Value.toString()));
      else
      if ( typeof Value !== "string")
      {
          console.log( "CmdWeird", this.Cmd, Value);
          this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), Value.toString() + "weird"));
      }
      else
      if ( Value.length === 0)
          this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
      else
      if ( Value === "''")
          this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
      else
        // if Escape string E' use \, otherwise use doubled quote ''
        // http://stackoverflow.com/questions/935/string-literals-and-escape-characters-in-postgresql
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), Value));
      return this;
  }
  SetParamData( pos, Value)
  {
    if ( Value == null)
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
    else
    if ( typeof Value == "undefined")
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "NULL");
    else
    {
      invariant( typeof Value === "object")
      invariant( !Array.isArray( Value))
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), JSON.stringify( Value)));
    }
    return this;
  }
  SetParamJSONData( pos, Value)
  {
    // alias
    this.SetParamData( pos, Value);
  }
  SetParamJSON( pos, Value)
  {
    // alias
    this.SetParamData( pos, Value);
  }
  SetParamInt( pos, Value)
  {
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", String( Value));
      return this;
  }
  SetParamBool( pos, Value)
  {
      if ( Value)
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "TRUE");
      else
        this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", "FALSE");
      return this;
  }
  SetParamDate( pos, Value)
  {
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectString( this.getDialect(), Value.toISOString()));
      return this;
  }
  SetParamName( pos, Value)
  {
      this.Cmd = this.Cmd.replaceAll( "%" + String( pos) + "%", SQLProtectName( this.getDialect(), Value));
      return this;
  }
}

export function SQLTag( DB)
{
  return function( strings, ...keys) {
    // could call SQLStripIndent
    // Offset of 100 allows a mixed usd of SQLTags and old style %2% + SetParam
    var result = strings[0];
    keys.forEach(function(key, i) {
      result += "%" + String( i + 100) + '%' + strings[i + 1];
    })
    let Cmd = new KNVSQLCommand( result);
    Cmd.SetDriver( DB);
    keys.forEach(function(key, i) {
      let value = key;
      if ( Number.isInteger( value))
        Cmd.SetParamInt( i + 100, value);
      else
      if ( typeof value == "string")
      {
        if ( value.startsWith( 'SELECT '))
          Cmd.SetParamRaw( i + 100, value);
        else
          Cmd.SetParamNull( i + 100, value);
      }
      else
      if ( typeof value == "symbol")
      {
        console.log( "case symbol", value.toString());
        let value2 = value.toString().replace( /^Symbol\((.*)\)$/, "$1");
        console.log( "value2", value2);
        // It can be pieces of Names like "Backup${Symbol("Items")}"
        Cmd.SetParamRaw( i + 100, value2);
      }
      else
      if ( typeof value == "boolean")
        Cmd.SetParamBool( i + 100, value);
      else
      if ( typeof value == "object")
        if ( value instanceof Array) 
          Cmd.SetParamRaw( i + 100, "ARRAY[" + value.join(",") + "]");
        else
        if ( value instanceof Date) 
          Cmd.SetParamDate( i + 100, value);
        else
          Cmd.SetParamData( i + 100, value);
      else
        Cmd.SetParamNull( i + 100, value);
    })
    return Cmd;
  }
};


