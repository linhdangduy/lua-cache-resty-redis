--[[

  Copyright (C) 2014 Masatoshi Teruya

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:
 
  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.
 
  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 
  redis.lua
  lua-cache-resty-redis
  
  Created by Masatoshi Teruya on 14/11/07.
  
--]]

-- modules
local Cache = require('cache');
local redis = require('resty.redis');
local encode = require('cjson.safe').encode;
local decode = require('cjson.safe').decode;
local typeof = require('util').typeof;
local unpack = unpack or table.unpack;
-- constants
local NULL = ngx.null;
local DEFAULT_HOST = '127.0.0.1';
local DEFAULT_PORT = 6379;
local DEFAULT_OPTS = {
    -- connect timeout
    timeout = 1000,
    -- unlimited
    idle = 0,
    -- pool size
    pool = 1
};
-- errors
local EENCODE = 'encoding error: %q';
local EEXEC = 'execution error: %q';

-- connection class
local RedisConn = require('halo').class.RedisConn;


function RedisConn:init( host, port, opts )
    local own = protected(self);
    local opt;
    
    host = host or DEFAULT_HOST;
    port = port or DEFAULT_PORT;
    if not typeof.string( host ) then
        return nil, 'host must be string';
    elseif not typeof.uint( port ) then
        return nil, 'port must be uint';
    elseif not opts then
        opts = {};
    elseif not typeof.table( opts ) then
        return nil, 'opts must be table';
    end
    
    own.host = host;
    own.port = port;
    
    for k, v in pairs( DEFAULT_OPTS ) do
        opt = opts[k];
        if opt == nil then
            own[k] = v;
        elseif not typeof.uint( opt ) then
            return nil, ('%s must be uint'):format( k );
        else
            own[k] = opt;
        end
    end
    
    return self;
end


function RedisConn:exec( qry )
    local own = protected(self);
    local db = redis.new();
    local ok, err, res, kerr;
    
    -- set option
    db:set_timeout( own.timeout );
    ok, err = db:connect( own.host, own.port );
    if not ok then
        return nil, EEXEC:format( err );
    end
    
    -- exec
    for _, qry in ipairs( qry ) do
        res, err = db[qry[1]]( db, select( 2, unpack( qry ) ) );
        if err then
            if _ > 1 then
                db:discard();
            end
            err = EEXEC:format( err );
            break;
        end
    end
    
    -- keepalive
    ok, kerr = db:set_keepalive( own.idle, own.pool );
    
    return res, err, kerr;
end


RedisConn = RedisConn.exports;


-- cache class
local CacheRedis = require('halo').class.CacheRedis;


function CacheRedis:init( host, port, opts, ttl )
    local own = protected(self);
    local err;
    
    own.conn, err = RedisConn.new( host, port, opts );
    if err then
        return nil, err;
    end
    
    return Cache.new( self, ttl );
end


function CacheRedis:set( key, val, ttl )
    local ok, err, kerr;
    
    val, err = encode( val );
    -- should encode
    if err then
        return false, EENCODE:format( err );
    -- exec with ttl
    elseif ttl > 0 then
        ok, err, kerr = protected(self).conn:exec({
            { 'multi' },
            { 'set', key, val },
            { 'expire', key, ttl },
            { 'exec' }
        });
    -- exec no ttl
    else
        ok, err, kerr = protected(self).conn:exec({
            { 'set', key, val }
        });
    end
    
    return err == nil, err, kerr;
end


function CacheRedis:get( key )
    local res, err = protected(self).conn:exec({
        { 'get', key }
    });
    
    if err then
        return nil, err;
    elseif res == NULL then
        res = nil;
    else
        res, err = decode( res );
        if err then
            return nil, err;
        end
    end
    
    return res;
end


function CacheRedis:delete( key )
    local _, err = protected(self).conn:exec({
        { 'delete', key }
    });
    
    if err then
        return false, err;
    end
    
    return true;
end


return CacheRedis.exports;
