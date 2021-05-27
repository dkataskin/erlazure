%% Copyright (c) 2013 - 2015, Dmitry Kataskin
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
%% * Neither the name of erlazure nor the names of its contributors may be used to
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

%% ============================================================================
%% Azure Storage API.
%% ============================================================================

-module(erlazure).
-author("Dmitry Kataskin").

-include("../include/erlazure.hrl").

-define(json_content_type, "application/json").
-define(gen_server_call_default_timeout, 30000).

-behaviour(gen_server).

%% API
-export([start/2]).

%% Queue API
-export([list_queues/1, list_queues/2, list_queues/3]).
-export([set_queue_acl/3, set_queue_acl/4, set_queue_acl/5]).
-export([get_queue_acl/2, get_queue_acl/3, get_queue_acl/4]).
-export([create_queue/2, create_queue/3, create_queue/4]).
-export([delete_queue/2, delete_queue/3, delete_queue/4]).
-export([put_message/3, put_message/4, put_message/5]).
-export([get_messages/2, get_messages/3, get_messages/4]).
-export([peek_messages/2, peek_messages/3, peek_messages/4]).
-export([delete_message/4, delete_message/5, delete_message/6]).
-export([clear_messages/2, clear_messages/3, clear_messages/4]).
-export([update_message/4, update_message/5, update_message/6]).

%% Blob API
-export([list_containers/1, list_containers/2, list_containers/3]).
-export([create_container/2, create_container/3, create_container/4]).
-export([delete_container/2, delete_container/3, delete_container/4]).
-export([lease_container/3, lease_container/4, lease_container/5]).
-export([list_blobs/2, list_blobs/3, list_blobs/4]).
-export([put_block_blob/4, put_block_blob/5, put_block_blob/6]).
-export([put_page_blob/4, put_page_blob/5, put_page_blob/6]).
-export([get_blob/3, get_blob/4, get_blob/5]).
-export([snapshot_blob/3, snapshot_blob/4, snapshot_blob/5]).
-export([copy_blob/4, copy_blob/5, copy_blob/6]).
-export([delete_blob/3, delete_blob/4, delete_blob/5]).
-export([put_block/5, put_block/6, put_block/7]).
-export([put_block_list/4, put_block_list/5, put_block_list/6]).
-export([get_block_list/3, get_block_list/4, get_block_list/5]).
-export([acquire_blob_lease/4, acquire_blob_lease/5, acquire_blob_lease/7]).

%% Table API
-export([list_tables/1, list_tables/3, new_table/2, delete_table/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, { account = "", key = "", options = [], param_specs = [] }).

%%====================================================================
%% API
%%====================================================================

-spec start(string(), string()) -> {ok, pid()}.
start(Account, Key) ->
        gen_server:start_link(?MODULE, {Account, Key}, []).

%%====================================================================
%% Queue
%%====================================================================

-spec list_queues(pid()) -> enum_parse_result(queue()).
list_queues(Pid) ->
        list_queues(Pid, []).

-spec list_queues(pid(), common_opts()) -> enum_parse_result(queue()).
list_queues(Pid, Options) ->
        list_queues(Pid, Options, ?gen_server_call_default_timeout).

-spec list_queues(pid(), common_opts(), pos_integer()) -> enum_parse_result(queue()).
list_queues(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {list_queues, Options}, Timeout).

-type queue_acl_opts() :: req_param_timeout() | req_param_clientrequestid().
-spec set_queue_acl(pid(), string(), signed_id()) -> {ok, created}.
set_queue_acl(Pid, Queue, SignedId=#signed_id{}) ->
        set_queue_acl(Pid, Queue, SignedId, []).

-spec set_queue_acl(pid(), string(), signed_id(), list(queue_acl_opts())) -> {ok, created}.
set_queue_acl(Pid, Queue, SignedId=#signed_id{}, Options) ->
        set_queue_acl(Pid, Queue, SignedId, Options, ?gen_server_call_default_timeout).

-spec set_queue_acl(pid(), string(), signed_id(), list(queue_acl_opts()), pos_integer()) -> {ok, created}.
set_queue_acl(Pid, Queue, SignedId=#signed_id{}, Options, Timeout) when is_list(Options); is_integer(Timeout)->
        gen_server:call(Pid, {set_queue_acl, Queue, SignedId, Options}, Timeout).

-spec get_queue_acl(pid(), string()) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue) ->
        get_queue_acl(Pid, Queue, []).

-spec get_queue_acl(pid(), string(), list(queue_acl_opts())) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue, Options) ->
        get_queue_acl(Pid, Queue, Options, ?gen_server_call_default_timeout).

-spec get_queue_acl(pid(), string(), list(queue_acl_opts()), pos_integer()) -> {ok, no_acl} | {ok, signed_id()}.
get_queue_acl(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {get_queue_acl, Queue, Options}, Timeout).

-spec create_queue(pid(), string()) -> created_response() | already_created_response().
create_queue(Pid, Queue) ->
        create_queue(Pid, Queue, []).
create_queue(Pid, Queue, Options) ->
        create_queue(Pid, Queue, Options, ?gen_server_call_default_timeout).
create_queue(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {create_queue, Queue, Options}, Timeout).

delete_queue(Pid, Queue) ->
        delete_queue(Pid, Queue, []).
delete_queue(Pid, Queue, Options) ->
        delete_queue(Pid, Queue, Options, ?gen_server_call_default_timeout).
delete_queue(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {delete_queue, Queue, Options}, Timeout).

put_message(Pid, Queue, Message) ->
        put_message(Pid, Queue, Message, []).
put_message(Pid, Queue, Message, Options) ->
        put_message(Pid, Queue, Message, Options, ?gen_server_call_default_timeout).
put_message(Pid, Queue, Message, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {put_message, Queue, Message, Options}, Timeout).

get_messages(Pid, Queue) ->
        get_messages(Pid, Queue, []).
get_messages(Pid, Queue, Options) ->
        get_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
get_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {get_messages, Queue, Options}, Timeout).

peek_messages(Pid, Queue) ->
        peek_messages(Pid, Queue, []).
peek_messages(Pid, Queue, Options) ->
        peek_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
peek_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {peek_messages, Queue, Options}, Timeout).

delete_message(Pid, Queue, MessageId, PopReceipt) ->
        delete_message(Pid, Queue, MessageId, PopReceipt, []).
delete_message(Pid, Queue, MessageId, PopReceipt, Options)  ->
        delete_message(Pid, Queue, MessageId, PopReceipt, Options, ?gen_server_call_default_timeout).
delete_message(Pid, Queue, MessageId, PopReceipt, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {delete_message, Queue, MessageId, PopReceipt, Options}, Timeout).

clear_messages(Pid, Queue) ->
        clear_messages(Pid, Queue, []).
clear_messages(Pid, Queue, Options) ->
        clear_messages(Pid, Queue, Options, ?gen_server_call_default_timeout).
clear_messages(Pid, Queue, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {clear_messages, Queue, Options}, Timeout).

update_message(Pid, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout) ->
        update_message(Pid, Queue, UpdatedMessage, VisibilityTimeout, []).
update_message(Pid, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options) ->
        update_message(Pid, Queue, UpdatedMessage, VisibilityTimeout, Options, ?gen_server_call_default_timeout).
update_message(Pid, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {update_message, Queue, UpdatedMessage, VisibilityTimeout, Options}, Timeout).

%%====================================================================
%% Blob
%%====================================================================

list_containers(Pid) ->
        list_containers(Pid, []).
list_containers(Pid, Options) ->
        list_containers(Pid, Options, ?gen_server_call_default_timeout).
list_containers(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {list_containers, Options}, Timeout).

create_container(Pid, Name) ->
        create_container(Pid, Name, []).
create_container(Pid, Name, Options) ->
        create_container(Pid, Name, Options, ?gen_server_call_default_timeout).
create_container(Pid, Name, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {create_container, Name, Options}, Timeout).

delete_container(Pid, Name) ->
        delete_container(Pid, Name, []).
delete_container(Pid, Name, Options) ->
        delete_container(Pid, Name, Options, ?gen_server_call_default_timeout).
delete_container(Pid, Name, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {delete_container, Name, Options}, Timeout).

put_block_blob(Pid, Container, Name, Data) ->
        put_block_blob(Pid, Container, Name, Data, []).
put_block_blob(Pid, Container, Name, Data, Options) ->
        put_block_blob(Pid, Container, Name, Data, Options, ?gen_server_call_default_timeout).
put_block_blob(Pid, Container, Name, Data, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {put_blob, Container, Name, block_blob, Data, Options}, Timeout).

put_page_blob(Pid, Container, Name, ContentLength) ->
        put_page_blob(Pid, Container, Name, ContentLength, []).
put_page_blob(Pid, Container, Name, ContentLength, Options) ->
        put_page_blob(Pid, Container, Name, ContentLength, Options, ?gen_server_call_default_timeout).
put_page_blob(Pid, Container, Name, ContentLength, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {put_blob, Container, Name, page_blob, ContentLength, Options}, Timeout).

list_blobs(Pid, Container) ->
        list_blobs(Pid, Container, []).
list_blobs(Pid, Container, Options) ->
        list_blobs(Pid, Container, Options, ?gen_server_call_default_timeout).
list_blobs(Pid, Container, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {list_blobs, Container, Options}, Timeout).

get_blob(Pid, Container, Blob) ->
        get_blob(Pid, Container, Blob, []).
get_blob(Pid, Container, Blob, Options) ->
        get_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
get_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {get_blob, Container, Blob, Options}, Timeout).

snapshot_blob(Pid, Container, Blob) ->
        snapshot_blob(Pid, Container, Blob, []).
snapshot_blob(Pid, Container, Blob, Options) ->
        snapshot_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
snapshot_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {snapshot_blob, Container, Blob, Options}, Timeout).

copy_blob(Pid, Container, Blob, Source) ->
        copy_blob(Pid, Container, Blob, Source, []).
copy_blob(Pid, Container, Blob, Source, Options) ->
        copy_blob(Pid, Container, Blob, Source, Options, ?gen_server_call_default_timeout).
copy_blob(Pid, Container, Blob, Source, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {copy_blob, Container, Blob, Source, Options}, Timeout).

delete_blob(Pid, Container, Blob) ->
        delete_blob(Pid, Container, Blob, []).
delete_blob(Pid, Container, Blob, Options) ->
        delete_blob(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
delete_blob(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {delete_blob, Container, Blob, Options}, Timeout).

put_block(Pid, Container, Blob, BlockId, BlockContent) ->
        put_block(Pid, Container, Blob, BlockId, BlockContent, []).
put_block(Pid, Container, Blob, BlockId, BlockContent, Options) ->
        put_block(Pid, Container, Blob, BlockId, BlockContent, Options, ?gen_server_call_default_timeout).
put_block(Pid, Container, Blob, BlockId, BlockContent, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {put_block, Container, Blob, BlockId, BlockContent, Options}, Timeout).

put_block_list(Pid, Container, Blob, BlockRefs) ->
        put_block_list(Pid, Container, Blob, BlockRefs, []).
put_block_list(Pid, Container, Blob, BlockRefs, Options) ->
        put_block_list(Pid, Container, Blob, BlockRefs, Options, ?gen_server_call_default_timeout).
put_block_list(Pid, Container, Blob, BlockRefs, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {put_block_list, Container, Blob, BlockRefs, Options}, Timeout).

get_block_list(Pid, Container, Blob) ->
        get_block_list(Pid, Container, Blob, []).
get_block_list(Pid, Container, Blob, Options) ->
        get_block_list(Pid, Container, Blob, Options, ?gen_server_call_default_timeout).
get_block_list(Pid, Container, Blob, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {get_block_list, Container, Blob, Options}, Timeout).

acquire_blob_lease(Pid, Container, Blob, Duration) ->
        acquire_blob_lease(Pid, Container, Blob, Duration, []).
acquire_blob_lease(Pid, Container, Blob, Duration, Options) ->
        acquire_blob_lease(Pid, Container, Blob, "", Duration, Options, ?gen_server_call_default_timeout).
acquire_blob_lease(Pid, Container, Blob, ProposedId, Duration, Options, Timeout) when is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {acquire_blob_lease, Container, Blob, ProposedId, Duration, Options}, Timeout).

lease_container(Pid, Name, Mode) ->
        lease_container(Pid, Name, Mode, []).
lease_container(Pid, Name, Mode, Options) ->
        lease_container(Pid, Name, Mode, Options, ?gen_server_call_default_timeout).
lease_container(Pid, Name, Mode, Options, Timeout) when is_atom(Mode); is_list(Options); is_integer(Timeout) ->
        gen_server:call(Pid, {lease_container, Name, Mode, Options}, Timeout).

%%====================================================================
%% Table
%%====================================================================

list_tables(Pid) ->
        list_tables(Pid, [], ?gen_server_call_default_timeout).
list_tables(Pid, Options, Timeout) when is_list(Options); is_integer(Timeout)->
        gen_server:call(Pid, {list_tables, Options}, Timeout).

new_table(Pid, TableName) when is_list(TableName) ->
        new_table(Pid, list_to_binary(TableName));

new_table(Pid, TableName) when is_binary(TableName) ->
        gen_server:call(Pid, {new_table, TableName}).

delete_table(Pid, TableName) when is_binary(TableName) ->
        delete_table(Pid, binary_to_list(TableName));

delete_table(Pid, TableName) when is_list(TableName) ->
        gen_server:call(Pid, {delete_table, TableName}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Account, Key}) ->
        {ok, #state { account = Account,
                      key = Key,
                      param_specs = get_req_param_specs() }}.

% List queues
handle_call({list_queues, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{params, [{comp, list}] ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        ParseResult = erlazure_queue:parse_queue_list(Body),
        {reply, ParseResult, State};

% Set queue acl
handle_call({set_queue_acl, Queue, SignedId=#signed_id{}, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, put},
                      {path, string:to_lower(Queue)},
                      {body, erlazure_queue:get_request_body(set_queue_acl, SignedId)},
                      {params, [{comp, acl}] ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, created);

% Get queue acl
handle_call({get_queue_acl, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue)},
                      {params, [{comp, acl}] ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        ParseResult = erlazure_queue:parse_queue_acl_response(Body),
        {reply, ParseResult, State};

% Create queue
handle_call({create_queue, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, put},
                      {path, string:to_lower(Queue)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, _Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_created ->
            {reply, {ok, created}, State};
          ?http_no_content ->
            {reply, {error, already_created}, State}
        end;

% Delete queue
handle_call({delete_queue, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, string:to_lower(Queue)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted);

% Add message to a queue
handle_call({put_message, Queue, Message, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, post},
                      {path, lists:concat([string:to_lower(Queue), "/messages"])},
                      {body, erlazure_queue:get_request_body(put_message, Message)},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created);

% Get messages from the queue
handle_call({get_messages, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue) ++ "/messages"},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {reply, erlazure_queue:parse_queue_messages_list(Body), State};

% Peek messages from the queue
handle_call({peek_messages, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{path, string:to_lower(Queue) ++ "/messages"},
                      {params, [{peek_only, true}] ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {reply, erlazure_queue:parse_queue_messages_list(Body), State};

% Delete message from the queue
handle_call({delete_message, Queue, MessageId, PopReceipt, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, lists:concat([string:to_lower(Queue), "/messages/", MessageId])},
                      {params, [{pop_receipt, PopReceipt}] ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted);

% Delete all messages from the queue
handle_call({clear_messages, Queue, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        ReqOptions = [{method, delete},
                      {path, string:to_lower(Queue) ++ "/messages"},
                      {params, Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, deleted);

% Update a message in the queue
handle_call({update_message, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options}, _From, State) ->
        ServiceContext = new_service_context(?queue_service, State),
        Params = [{pop_receipt, UpdatedMessage#queue_message.pop_receipt},
                  {message_visibility_timeout, integer_to_list(VisibilityTimeout)}],
        ReqOptions = [{method, put},
                      {path, lists:concat([string:to_lower(Queue), "/messages/", UpdatedMessage#queue_message.id])},
                      {body, erlazure_queue:get_request_body(update_message, UpdatedMessage#queue_message.text)},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?queue_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_no_content, updated);

% List containers
handle_call({list_containers, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{params, [{comp, list}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, Containers} = erlazure_blob:parse_container_list(Body),
        {reply, Containers, State};

% Create a container
handle_call({create_container, Name, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, Name},
                      {params, [{res_type, container}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),
        {Code, Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_created -> {reply, {ok, created}, State};
          _ -> {reply, {error, Body}, State}
        end;

% Delete container
handle_call({delete_container, Name, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, delete},
                      {path, Name},
                      {params, [{res_type, container}] ++ Options}],
        RequestContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, RequestContext),
        return_response(Code, Body, State, ?http_accepted, deleted);

% Lease a container
handle_call({lease_container, Name, Mode, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, lease},
                  {res_type, container},
                  {lease_action, Mode}],
        ReqOptions = [{method, put},
                      {path, Name},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, deleted);

% List blobs
handle_call({list_blobs, Name, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, list},
                  {res_type, container}],
        ReqOptions = [{path, Name},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, Blobs} = erlazure_blob:parse_blob_list(Body),
        {reply, Blobs, State};

% Put block blob
handle_call({put_blob, Container, Name, Type = block_blob, Data, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {body, Data},
                      {params, [{blob_type, Type}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),
        ReqContext1 = case proplists:get_value(content_type, Options) of
                        undefined    -> ReqContext#req_context{ content_type = "application/octet-stream" };
                        ContentType  -> ReqContext#req_context{ content_type = ContentType }
                      end,

        {Code, Body} = execute_request(ServiceContext, ReqContext1),
        return_response(Code, Body, State, ?http_created, created);

% Put page blob
handle_call({put_blob, Container, Name, Type = page_blob, ContentLength, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{blob_type, Type},
                  {blob_content_length, ContentLength}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Name])},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created);

% Get blob
handle_call({get_blob, Container, Blob, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{path, lists:concat([Container, "/", Blob])},
                      {params, Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        case Code of
          ?http_ok ->
            {reply, {ok, Body}, State};
          ?http_partial_content->
            {reply, {ok, Body}, State};
          _ -> {reply, {error, Body}, State}
        end;

% Snapshot blob
handle_call({snapshot_blob, Container, Blob, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, [{comp, snapshot}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created);

% Copy blob
handle_call({copy_blob, Container, Blob, Source, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, [{blob_copy_source, Source}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, created);

% Delete blob
handle_call({delete_blob, Container, Blob, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, delete},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_accepted, deleted);

% Put block
handle_call({put_block, Container, Blob, BlockId, Content, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        Params = [{comp, block},
                  {blob_block_id, base64:encode_to_string(BlockId)}],
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {body, Content},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created);

% Put block list
handle_call({put_block_list, Container, Blob, BlockRefs, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {body, erlazure_blob:get_request_body(BlockRefs)},
                      {params, [{comp, "blocklist"}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, created);

% Get block list
handle_call({get_block_list, Container, Blob, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),
        ReqOptions = [{path, lists:concat([Container, "/", Blob])},
                      {params, [{comp, "blocklist"}] ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {ok, BlockList} = erlazure_blob:parse_block_list(Body),
        {reply, BlockList, State};

% Acquire blob lease
handle_call({acquire_blob_lease, Container, Blob, ProposedId, Duration, Options}, _From, State) ->
        ServiceContext = new_service_context(?blob_service, State),

        Params = [{lease_action, acquire},
                  {proposed_lease_id, ProposedId},
                  {lease_duration, Duration},
                  {comp, lease}],

        ReqOptions = [{method, put},
                      {path, lists:concat([Container, "/", Blob])},
                      {params, Params ++ Options}],
        ReqContext = new_req_context(?blob_service, State#state.account, State#state.param_specs, ReqOptions),

        {Code, Body} = execute_request(ServiceContext, ReqContext),
        return_response(Code, Body, State, ?http_created, acquired);

% List tables
handle_call({list_tables, Options}, _From, State) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, "Tables"},
                      {params, Options}],
        ReqContext = new_req_context(?table_service, State#state.account, State#state.param_specs, ReqOptions),

        {?http_ok, Body} = execute_request(ServiceContext, ReqContext),
        {reply, {ok, erlazure_table:parse_table_list(Body)}, State};

% New tables
handle_call({new_table, TableName}, _From, State) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, "Tables"},
                      {method, post},
                      {body, jsx:encode([{<<"TableName">>, TableName}])}],
        ReqContext = new_req_context(?table_service, State#state.account, State#state.param_specs, ReqOptions),
        ReqContext1 = ReqContext#req_context{ content_type = ?json_content_type },
        {Code, Body} = execute_request(ServiceContext, ReqContext1),
        return_response(Code, Body, State, ?http_created, created);

% Delete table
handle_call({delete_table, TableName}, _From, State) ->
        ServiceContext = new_service_context(?table_service, State),
        ReqOptions = [{path, io:format("Tables('~s')", [TableName])},
                      {method, delete}],
        ReqContext = new_req_context(?table_service, State#state.account, State#state.param_specs, ReqOptions),
        {?http_no_content, _} = execute_request(ServiceContext, ReqContext),
        {reply, {ok, deleted}, State}.

handle_cast(_Msg, State) ->
        {noreply, State}.

handle_info(_Info, State) ->
        {noreply, State}.

terminate(_Reason, _State) ->
        ok.

code_change(_OldVer, State, _Extra) ->
        {ok, State}.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

-spec execute_request(service_context(), req_context()) -> {non_neg_integer(), binary()}.
execute_request(ServiceContext = #service_context{}, ReqContext = #req_context{}) ->
        DateHeader = if (ServiceContext#service_context.service =:= ?table_service) ->
                          {"Date", httpd_util:rfc1123_date()};
                        true ->
                          {"x-ms-date", httpd_util:rfc1123_date()}
                     end,

        Headers =  [DateHeader,
                    {"x-ms-version", ServiceContext#service_context.api_version},
                    {"Host", get_host(ServiceContext#service_context.service,
                                      ServiceContext#service_context.account)}],

        Headers1 = if (ReqContext#req_context.method =:= put orelse
                       ReqContext#req_context.method =:= post) ->
                        ContentHeaders = [{"Content-Type", ReqContext#req_context.content_type},
                                          {"Content-Length", integer_to_list(ReqContext#req_context.content_length)}],
                        lists:append([Headers, ContentHeaders, ReqContext#req_context.headers]);

                      true ->
                        lists:append([Headers, ReqContext#req_context.headers])
                   end,

        AuthHeader = {"Authorization", get_shared_key(ServiceContext#service_context.service,
                                                      ServiceContext#service_context.account,
                                                      ServiceContext#service_context.key,
                                                      ReqContext#req_context.method,
                                                      ReqContext#req_context.path,
                                                      ReqContext#req_context.parameters,
                                                      Headers1)},

        %% Fiddler
        %% httpc:set_options([{ proxy, {{"localhost", 9999}, []}}]),

        Response = httpc:request(ReqContext#req_context.method,
                                 erlazure_http:create_request(ReqContext, [AuthHeader | Headers1]),
                                 [{version, "HTTP/1.1"}, {ssl, [{versions, ['tlsv1.2']}]}],
                                 [{sync, true}, {body_format, binary}, {headers_as_is, true}]),
        case Response of
          {ok, {{_, Code, _}, _, Body}}
          when Code >= 200, Code =< 206 ->
            {Code, Body};

          {ok, {{_, _, _}, _, Body}} ->
            try get_error_code(Body) of
              ErrorCodeAtom -> {error, ErrorCodeAtom}
              catch
                _ -> {error, Body}
              end
           end.

get_error_code(Body) ->
        {ParseResult, _} = xmerl_scan:string(binary_to_list(Body)),
        ErrorContent = ParseResult#xmlElement.content,
        ErrorContentHead = hd(ErrorContent),
        CodeContent = ErrorContentHead#xmlElement.content,
        CodeContentHead = hd(CodeContent),
        ErrorCodeText = CodeContentHead#xmlText.value,
        list_to_atom(ErrorCodeText).

get_shared_key(Service, Account, Key, HttpMethod, Path, Parameters, Headers) ->
        SignatureString = get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters),
        "SharedKey " ++ Account ++ ":" ++ base64:encode_to_string(sign_string(Key, SignatureString)).

get_signature_string(Service, HttpMethod, Headers, Account, Path, Parameters) ->
        SigStr1 = erlazure_http:verb_to_str(HttpMethod) ++ "\n" ++
                  get_headers_string(Service, Headers),

        SigStr2 = if (Service =:= ?queue_service) orelse (Service =:= ?blob_service) ->
                    SigStr1 ++ canonicalize_headers(Headers);
                    true -> SigStr1
                  end,
        SigStr2 ++ canonicalize_resource(Account, Path, Parameters).

get_headers_string(Service, Headers) ->
        FoldFun = fun(HeaderName, Acc) ->
                    case lists:keyfind(HeaderName, 1, Headers) of
                      {HeaderName, Value} -> lists:concat([Acc, Value, "\n"]);
                      false -> lists:concat([Acc, "\n"])
                    end
                  end,
        lists:foldl(FoldFun, "", get_header_names(Service)).

-spec sign_string(base64:ascii_string(), string()) -> binary().
-ifdef(OTP_RELEASE).
-if(OTP_RELEASE >= 23).
sign_string(Key, StringToSign) ->
  crypto:mac(hmac, sha256, base64:decode(Key), StringToSign).
-else.
sign_string(Key, StringToSign) ->
  crypto:hmac(sha256, base64:decode(Key), StringToSign).
-endif.
-else.
sign_string(Key, StringToSign) ->
  crypto:hmac(sha256, base64:decode(Key), StringToSign).
-endif.

build_uri_base(Service, Account) ->
        lists:concat(["https://", get_host(Service, Account), "/"]).

get_host(Service, Account) ->
        lists:concat([Account, ".", erlang:atom_to_list(Service), ".core.windows.net"]).

-spec canonicalize_headers([string()]) -> string().
canonicalize_headers(Headers) ->
        MSHeaderNames = [HeaderName || {HeaderName, _} <- Headers, string:str(HeaderName, "x-ms-") =:= 1],
        SortedHeaderNames = lists:sort(MSHeaderNames),
        FoldFun = fun(HeaderName, Acc) ->
                    {_, Value} = lists:keyfind(HeaderName, 1, Headers),
                    lists:concat([Acc, HeaderName, ":", Value, "\n"])
                  end,
        lists:foldl(FoldFun, "", SortedHeaderNames).

canonicalize_resource(Account, Path, []) ->
        lists:concat(["/", Account, "/", Path]);

canonicalize_resource(Account, Path, Parameters) ->
        SortFun = fun({ParamNameA, ParamValA}, {ParamNameB, ParamValB}) ->
                    ParamNameA ++ ParamValA =< ParamNameB ++ ParamValB
                 end,
        SortedParameters = lists:sort(SortFun, Parameters),
        [H | T] = SortedParameters,
        "/" ++ Account ++ "/" ++ Path ++ combine_canonical_param(H, "", "", T).

combine_canonical_param({Param, Value}, Param, Acc, []) ->
        add_value(Value, Acc);

combine_canonical_param({Param, Value}, _PreviousParam, Acc, []) ->
        add_param_value(Param, Value, Acc);

combine_canonical_param({Param, Value}, Param, Acc, ParamList) ->
        [H | T] = ParamList,
        combine_canonical_param(H, Param, add_value(Value, Acc), T);

combine_canonical_param({Param, Value}, _PreviousParam, Acc, ParamList) ->
        [H | T] = ParamList,
        combine_canonical_param(H, Param, add_param_value(Param, Value, Acc), T).

add_param_value(Param, Value, Acc) ->
        Acc ++ "\n" ++ string:to_lower(Param) ++ ":" ++ Value.

add_value(Value, Acc) ->
        Acc ++ "," ++ Value.

get_header_names(?blob_service) ->
        get_header_names(?queue_service);

get_header_names(?queue_service) ->
        ["Content-Encoding",
         "Content-Language",
         "Content-Length",
         "Constent-MD5",
         "Content-Type",
         "Date",
         "If-Modified-Since",
         "If-Match",
         "If-None-Match",
         "If-Unmodified-Since",
         "Range"];

get_header_names(?table_service) ->
        ["Content-MD5",
         "Content-Type",
         "Date"].

new_service_context(?queue_service, State=#state{}) ->
        #service_context{ service = ?queue_service,
                          api_version = ?queue_service_ver,
                          account = State#state.account,
                          key = State#state.key };

new_service_context(?blob_service, State=#state{}) ->
        #service_context{ service = ?blob_service,
                          api_version = ?blob_service_ver,
                          account = State#state.account,
                          key = State#state.key };

new_service_context(?table_service, State=#state{}) ->
        #service_context{ service = ?table_service,
                          api_version = ?table_service_ver,
                          account = State#state.account,
                          key = State#state.key }.

new_req_context(Service, Account, ParamSpecs, Options) ->
        Method = proplists:get_value(method, Options, get),
        Path = proplists:get_value(path, Options, ""),
        Body = proplists:get_value(body, Options, ""),
        Headers = proplists:get_value(headers, Options, []),
        Params = proplists:get_value(params, Options, []),
        AddHeaders = if (Service =:= ?table_service) ->
                        case lists:keyfind("Accept", 1, Headers) of
                          false -> [{"Accept", "application/json;odata=fullmetadata"}];
                          _ -> []
                        end;
                        true -> []
                     end,

        ReqParams = get_req_uri_params(Params, ParamSpecs),
        ReqHeaders = lists:append([Headers, AddHeaders, get_req_headers(Params, ParamSpecs)]),

        #req_context{ address = build_uri_base(Service, Account),
                      path = Path,
                      method = Method,
                      body = Body,
                      content_length = erlazure_http:get_content_length(Body),
                      parameters = ReqParams,
                      headers = ReqHeaders }.

get_req_headers(Params, ParamSpecs) ->
        get_req_params(Params, ParamSpecs, header).

get_req_uri_params(Params, ParamSpecs) ->
        get_req_params(Params, ParamSpecs, uri).

get_req_params(Params, ParamSpecs, Type) ->
        ParamDefs = orddict:filter(fun(_, Value) -> Value#param_spec.type =:= Type end, ParamSpecs),
        FoldFun = fun({_ParamName, ""}, Acc) ->
                      Acc;

                      ({ParamName, ParamValue}, Acc) ->
                        case orddict:find(ParamName, ParamDefs) of
                          {ok, Value} -> [{Value#param_spec.name, (Value#param_spec.parse_fun)(ParamValue)} | Acc];
                          error -> Acc
                        end
                  end,
        lists:foldl(FoldFun, [], Params).

get_req_param_specs() ->
        ProcessFun = fun(Spec=#param_spec{}, Dictionary) ->
                        orddict:store(Spec#param_spec.id, Spec, Dictionary)
                    end,

        CommonParamSpecs = lists:foldl(ProcessFun, orddict:new(), get_req_common_param_specs()),
        BlobParamSpecs = lists:foldl(ProcessFun, CommonParamSpecs, erlazure_blob:get_request_param_specs()),

        lists:foldl(ProcessFun, BlobParamSpecs, erlazure_queue:get_request_param_specs()).

get_req_common_param_specs() ->
        [#param_spec{ id = comp, type = uri, name = "comp" },
         #param_spec{ id = ?req_param_timeout, type = uri, name = "timeout" },
         #param_spec{ id = ?req_param_maxresults, type = uri, name = "maxresults" },
         #param_spec{ id = ?req_param_prefix, type = uri, name = "prefix" },
         #param_spec{ id = ?req_param_include, type = uri, name = "include" },
         #param_spec{ id = ?req_param_marker, type = uri, name = "marker" }].

return_response(Code, Body, State, ExpectedResponseCode, SuccessAtom) ->
  case Code of
    ExpectedResponseCode ->
      {reply, {ok, SuccessAtom}, State};
    _ ->
      {reply, {error, Body}, State}
  end.
