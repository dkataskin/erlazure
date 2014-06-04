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

%% Request common parameters
-define(req_param_prefix, prefix).
-define(req_param_marker, marker).
-define(req_param_maxresults, max_results).
-define(req_param_include, include).
-define(req_param_timeout, timeout).

-include_lib("xmerl/include/xmerl.hrl").

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

%% Types
-type xmlElement() :: #xmlElement{}.
-export_type([xmlElement/0]).

-type metadata() :: proplists:proplist().
-export_type([metadata/0]).

-type method() :: get | post | delete | head.
-export_type([method/0]).

-type request_common_opts() :: {?req_param_prefix, string()}
              | {?req_param_marker, string()}
              | {?req_param_maxresults, non_neg_integer()}
              | {?req_param_include, metadata}
              | {?req_param_timeout, non_neg_integer()}.
-export_type([request_common_opts/0]).

-type lease_state() :: available | leased | breaking | broken | expired.
-type lease_status() :: locked | unlocked.
-type lease_duration() :: infinite | fixed.
-export_type([lease_state/0, lease_status/0, lease_duration/0]).

-type blob_block_type() :: committed | uncommitted.
-export_type([blob_block_type/0]).

-type enum_common_token() :: {prefix, string()}
              | {marker, string()}
              | {max_results, non_neg_integer()}
              | {delimiter, string()}
              | {next_marker, string()}.
-type enum_common_tokens() :: list(enum_common_token()).
-export_type([enum_common_token/0, enum_common_tokens/0]).

-type request_param_type() :: uri | header.
-export_type([request_param_type/0]).

-record(service_context, {service :: string(),
                          api_version :: string(),
                          account :: string(),
                          key :: string()}).
-type service_context() :: #service_context{}.
-export_type([service_context/0]).

-record(req_context, {method = get :: method(),
                      address :: string(),
                      path = "" :: string(),
                      parameters = [] :: list(),
                      content_type = "application/xml" :: string(),
                      content_length = 0 :: non_neg_integer(),
                      body = "" :: string(),
                      headers = [] :: [string()]}).
-type req_context() :: #req_context{}.
-export_type([req_context/0]).

-record(param_spec, {id :: atom(),
                     type :: request_param_type(),
                     name :: string(),
                     parse_fun = fun(Value) ->
                                   lists:flatten(io_lib:format("~p", [Value]))
                                 end :: fun((any()) -> string())}).
-type param_spec() :: #param_spec{}.
-export_type([param_spec/0]).

-record(property_spec, {name :: atom(),
                        key :: atom(),
                        parse_fun = fun(Elem=#xmlElement{}) ->
                                      erlazure_xml:parse_str(Elem)
                                    end :: fun((xmlElement()) -> string() | integer() | atom())}).
-type property_spec() :: #property_spec{}.
-export_type([property_spec/0]).

-record(enum_parser_spec, {rootKey :: atom(),
                           elementKey :: atom(),
                           elementParser :: any(),
                           customParsers=[] :: [any()]}).
-type enum_parser_spec() :: #enum_parser_spec{}.
-export_type([enum_parser_spec/0]).

% Queue
-record(queue, {name="" :: string(),
                url="" :: string(),
                metadata=[] :: list()}). %% @todo Improve specs.
-type queue() :: #queue{}.
-export_type([queue/0]).

-record(access_policy, {start="" :: string(),
                        expiry="" :: string(),
                        permission="" :: string()}).
-type access_policy() :: #access_policy{}.

-record(signed_id, {id="" :: string(),
                    access_policy=#access_policy{} :: access_policy()}).
-type signed_id() :: #signed_id{}.

-record(queue_message, {id="" :: string(),
                        insertion_time="" :: string(),
                        exp_time="" :: string(),
                        pop_receipt="" :: string(),
                        next_visible="" :: string(),
                        dequeue_count=0 :: non_neg_integer(),
                        text="" :: string()}).
-type queue_message() :: #queue_message{}.

% Blob
-record(blob_container, {name="" :: string(),
                         url="" :: string(),
                         properties=[] :: list(),
                         metadata=[] :: list()}).
-type blob_container() :: #blob_container{}.

-record(blob_lease, {id="" :: string(),
                     status :: lease_status(),
                     state :: lease_state(),
                     duration :: lease_duration()}).
-type blob_lease() :: #blob_lease{}.

-record(blob_copy_state, {id="",
                          status,
                          source="",
                          progress="",
                          completion_time="",
                          status_description=""}).

-record(cloud_blob, {name="" :: string(),
                     snapshot="" :: string(),
                     url="" :: string(),
                     properties=[] :: list(),
                     metadata=[] :: list()}).
-type cloud_blob() :: #cloud_blob{}.

-record(blob_block, {id="" :: string(),
                     type :: blob_block_type(),
                     size=0 :: non_neg_integer()}).
-type blob_block() :: #blob_block{}.