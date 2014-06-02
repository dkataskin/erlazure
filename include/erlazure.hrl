%% Copyright (c) 2013 - 2014, Dmitry Kataskin
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% * Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% * Redistributions in binary form must reproduce the above copyright
%% notice, this list of conditions and the following disclaimer in the
%% documentation and/or other materials provided with the distribution.
%% * Neither the name of  nor the names of its contributors may be used to
%% endorse or promote products derived from this software without specific
%% prior written permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
%% POSSIBILITY OF SUCH DAMAGE.

-author("Dmitry Kataskin").

-define(http_ok, 200).
-define(http_created, 201).
-define(http_accepted, 202).
-define(http_no_content, 204).

-define(blob_service, blob).
-define(table_service, table).
-define(queue_service, queue).

-define(queue_service_ver, "2012-02-12").
-define(blob_service_ver, "2012-02-12").
-define(table_service_ver, "2012-02-12").

-include_lib("xmerl/include/xmerl.hrl").

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

%% Types
-type xmlElement() :: #xmlElement{}.

-type method() :: get | post | delete | head.

-type requestParamType() :: uri | header.

-record(service_context, {service :: string(),
                          api_version :: string(),
                          account :: string(),
                          key :: string()}).
-type service_context() :: #service_context{}.

-record(req_context, {method = get :: method(),
                      address :: string(),
                      path = "" :: string(),
                      parameters = [] :: list(),
                      content_type = "application/xml" :: string(),
                      content_length = 0 :: non_neg_integer(),
                      body = "" :: string(),
                      headers = [] :: [string()]}).
-type req_context() :: #req_context{}.

-record(param_spec, {id :: atom(),
                     type :: requestParamType(),
                     name :: string(),
                     parse_fun = fun(Value) ->
                                   lists:flatten(io_lib:format("~p", [Value]))
                                 end :: fun((any()) -> string())}).
-type param_spec() :: #param_spec{}.

-record(property_spec, {name :: atom(),
                        key :: atom(),
                        parse_fun = fun(Elem=#xmlElement{}) ->
                                      erlazure_xml:parse_str(Elem)
                                    end :: fun((xmlElement()) -> string() | integer() | atom())}).

-record(enum_parser_spec, {rootKey :: atom(),
                           elementKey :: atom(),
                           elementParser :: any(),
                           customParsers=[] :: [any()]}).
-type enum_parser_spec() :: #enum_parser_spec{}.

% Queue
-record(queue, {name="" :: string(),
                url="" :: string(),
                metadata=[] :: list()}).

-record(access_policy, {start="",
                        expiry="",
                        permission=""}).

-record(signed_id, {id="",
                    access_policy=#access_policy{}}).

-record(queue_message, {id="" :: string(),
                        insertion_time="" :: string(),
                        exp_time="" :: string(),
                        pop_receipt="" :: string(),
                        next_visible="" :: string(),
                        dequeue_count=0 :: non_neg_integer(),
                        text="" :: string()}).
-type queue_message() :: #queue_message{}.

% Blob
-record(blob_container, {name="",
                         url="",
                         properties=[],
                         metadata=[]}).

-record(blob_lease, {id="",
                     status,
                     state,
                     duration}).

-record(blob_copy_state, {id="",
                          status,
                          source="",
                          progress="",
                          completion_time="",
                          status_description=""}).

-record(cloud_blob, {name="",
                     snapshot="",
                     url="",
                     properties=[],
                     metadata=[]}).

-record(blob_block, {id="",
                     type=unknown,
                     size=0}).