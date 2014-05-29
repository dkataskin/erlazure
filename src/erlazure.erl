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

-include("erlazure.hrl").

-behaviour(gen_server).

%% API
-export([start/2]).

%% Queue API
-export([list_queues/0, get_queue_acl/1, get_queue_acl/2, create_queue/1, create_queue/2,
         delete_queue/1, delete_queue/2, put_message/2, put_message/3, get_messages/1, get_messages/2,
         peek_messages/1, peek_messages/2, delete_message/3, delete_message/4, clear_messages/1,
         clear_messages/2, update_message/3, update_message/4]).

%% Blob API
-export([list_containers/0, list_containers/1, create_container/1, create_container/2, delete_container/1,
         delete_container/2, lease_container/2, lease_container/3, list_blobs/1, list_blobs/2, put_block_blob/3,
         put_block_blob/4, put_page_blob/3, put_page_blob/4, get_blob/2, get_blob/3, snapshot_blob/2,
         snapshot_blob/3, copy_blob/3, copy_blob/4, delete_blob/2, delete_blob/3, put_block/4,
         put_block/5, put_block_list/3, put_block_list/4, get_block_list/2, get_block_list/3,
         acquire_blob_lease/3, acquire_blob_lease/4, acquire_blob_lease/5]).

%% Table API
-export([get_tables/0, get_tables/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {account="", key="", options=[], param_specs =[]}).

%%====================================================================
%% API
%%====================================================================

start(Account, Key) ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, #state{account = Account,
                                                                    key = Key,
                                                                    param_specs = get_request_param_specs()}, []).

%%====================================================================
%% Queue
%%====================================================================
list_queues() ->
            list_queues([]).
list_queues(Options) when is_list(Options) ->
            gen_server:call(?MODULE, {list_queues, Options}).

get_queue_acl(Queue) ->
            get_queue_acl(Queue, []).
get_queue_acl(Queue, Options) when is_list(Options) ->
            gen_server:call(?MODULE, {get_queue_acl, Queue, Options}).

create_queue(Queue) ->
            create_queue(Queue, []).
create_queue(Queue, Options) when is_list(Options) ->
            gen_server:call(?MODULE, {create_queue, Queue, Options}).

delete_queue(Queue) ->
            delete_queue(Queue, []).
delete_queue(Queue, Options) when is_list(Options) ->
            gen_server:call(?MODULE, {delete_queue, Queue, Options}).

put_message(Queue, Message) ->
            put_message(Queue, Message, []).
put_message(Queue, Message, Options) when is_list(Options) ->
            gen_server:call(?MODULE, {put_message, Queue, Message, Options}).

get_messages(Queue) ->
            get_messages(Queue, []).
get_messages(Queue, Options) ->
            gen_server:call(?MODULE, {get_messages, Queue, Options}).

peek_messages(Queue) ->
            peek_messages(Queue, []).
peek_messages(Queue, Options) ->
            gen_server:call(?MODULE, {peek_messages, Queue, Options}).

delete_message(Queue, MessageId, PopReceipt) ->
            delete_message(Queue, MessageId, PopReceipt, []).
delete_message(Queue, MessageId, PopReceipt, Options) ->
            gen_server:call(?MODULE, {delete_message, Queue, MessageId, PopReceipt, Options}).

clear_messages(Queue) ->
            clear_messages(Queue, []).
clear_messages(Queue, Options) ->
            gen_server:call(?MODULE, {clear_messages, Queue, Options}).

update_message(Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout) ->
            update_message(Queue, UpdatedMessage, VisibilityTimeout, []).
update_message(Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options) ->
            gen_server:call(?MODULE, {update_message, Queue, UpdatedMessage, VisibilityTimeout, Options}).

%%====================================================================
%% Blob
%%====================================================================

list_containers() ->
            list_containers([]).
list_containers(Options) ->
            gen_server:call(?MODULE, {list_containers, Options}).

create_container(Name) ->
            create_container(Name, []).
create_container(Name, Options) ->
            gen_server:call(?MODULE, {create_container, Name, Options}).

delete_container(Name) ->
            delete_container(Name, []).
delete_container(Name, Options) ->
            gen_server:call(?MODULE, {delete_container, Name, Options}).

put_block_blob(Container, Name, Data) ->
            put_block_blob(Container, Name, Data, []).

put_block_blob(Container, Name, Data, Options) ->
            gen_server:call(?MODULE, {put_blob, Container, Name, block_blob, Data, Options}).

put_page_blob(Container, Name, ContentLength) ->
            put_block_blob(Container, Name, ContentLength, []).

put_page_blob(Container, Name, ContentLength, Options) ->
            gen_server:call(?MODULE, {put_blob, Container, Name, page_blob, ContentLength, Options}).

list_blobs(Container) ->
            list_blobs(Container, []).
list_blobs(Container, Options) ->
            gen_server:call(?MODULE, {list_blobs, Container, Options}).

get_blob(Container, Blob) ->
            get_blob(Container, Blob, []).
get_blob(Container, Blob, Options) ->
            gen_server:call(?MODULE, {get_blob, Container, Blob, Options}).

snapshot_blob(Container, Blob) ->
            snapshot_blob(Container, Blob, []).
snapshot_blob(Container, Blob, Options) ->
            gen_server:call(?MODULE, {snapshot_blob, Container, Blob, Options}).

copy_blob(Container, Blob, Source) ->
            copy_blob(Container, Blob, Source, []).
copy_blob(Container, Blob, Source, Options) ->
            gen_server:call(?MODULE, {copy_blob, Container, Blob, Source, Options}).

delete_blob(Container, Blob) ->
            delete_blob(Container, Blob, []).
delete_blob(Container, Blob, Options) ->
            gen_server:call(?MODULE, {delete_blob, Container, Blob, Options}).

put_block(Container, Blob, BlockId, BlockContent) ->
            put_block(Container, Blob, BlockId, BlockContent, []).
put_block(Container, Blob, BlockId, BlockContent, Options) ->
            gen_server:call(?MODULE, {put_block, Container, Blob, BlockId, BlockContent, Options}).

put_block_list(Container, Blob, BlockRefs) ->
            put_block_list(Container, Blob, BlockRefs, []).
put_block_list(Container, Blob, BlockRefs, Options) ->
            gen_server:call(?MODULE, {put_block_list, Container, Blob, BlockRefs, Options}).

get_block_list(Container, Blob) ->
            get_block_list(Container, Blob, []).
get_block_list(Container, Blob, Options) ->
            gen_server:call(?MODULE, {get_block_list, Container, Blob, Options}).

acquire_blob_lease(Container, Blob, Duration) ->
            acquire_blob_lease(Container, Blob, "", Duration, []).

acquire_blob_lease(Container, Blob, Duration, Options) ->
            acquire_blob_lease(Container, Blob, "", Duration, Options).

acquire_blob_lease(Container, Blob, ProposedId, Duration, Options) ->
            gen_server:call(?MODULE, {acquire_blob_lease, Container, Blob, ProposedId, Duration, Options}).

lease_container(Name, Mode) when is_atom(Mode) ->
            lease_container(Name, Mode, []).
lease_container(Name, Mode, Options) when is_atom(Mode) ->
            gen_server:call(?MODULE, {lease_container, Name, Mode, Options}).

%%====================================================================
%% Table
%%====================================================================

get_tables() ->
            get_tables([]).

get_tables(Options) ->
            gen_server:call(?MODULE, {get_table_list, Options}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(State) ->
            crypto:start(),
            inets:start(),
            ssl:start(),
            {ok, State}.

% List queues
handle_call({list_queues, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    [{comp, list}],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {ok, ParseResult} = erlazure_queue:parse_queue_list(Body),
            {reply, ParseResult, State};

% Get queue acl
handle_call({get_queue_acl, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    string:to_lower(Queue),
                                                    [{comp, acl}],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {reply, Body, State};

% Create queue
handle_call({create_queue, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    put,
                                                    string:to_lower(Queue),
                                                    [],
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Delete queue
handle_call({delete_queue, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    delete,
                                                    string:to_lower(Queue),
                                                    [],
                                                    Options),

            {?http_no_content, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% Add message to a queue
handle_call({put_message, Queue, Message, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    post,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    erlazure_queue:get_request_body(Message),
                                                    [],
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Get messages from the queue
handle_call({get_messages, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    [],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {reply, erlazure_queue:parse_queue_messages_list(Body), State};

% Peek messages from the queue
handle_call({peek_messages, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    [{peek_only, true}],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {reply, erlazure_queue:parse_queue_messages_list(Body), State};

% Delete message from the queue
handle_call({delete_message, Queue, MessageId, PopReceipt, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    delete,
                                                    string:to_lower(Queue) ++ "/messages/" ++ MessageId,
                                                    [{pop_receipt, PopReceipt}],
                                                    Options),

            {?http_no_content, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% Delete all messages from the queue
handle_call({clear_messages, Queue, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),
            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    delete,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    [],
                                                    Options),

            {?http_no_content, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% Update a message in the queue
handle_call({update_message, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options}, _From, State) ->
            ServiceContext = create_service_context(?queue_service, State),

            Parameters = [{pop_receipt, UpdatedMessage#queue_message.pop_receipt},
                          {message_visibility_timeout, integer_to_list(VisibilityTimeout)}],

            RequestContext = create_request_context(?queue_service,
                                                    State,
                                                    put,
                                                    string:to_lower(Queue) ++ "/messages/" ++ UpdatedMessage#queue_message.id,
                                                    erlazure_queue:get_request_body(UpdatedMessage#queue_message.text),
                                                    Parameters,
                                                    Options),

            {?http_no_content, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, updated}, State};

% List containers
handle_call({list_containers, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    [{comp, list}],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {ok, Containers} = erlazure_blob:parse_container_list(Body),
            {reply, Containers, State};

% Create a container
handle_call({create_container, Name, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Name,
                                                    [{res_type, container}],
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Delete container
handle_call({delete_container, Name, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    delete,
                                                    Name,
                                                    [{res_type, container}],
                                                    Options),

            {?http_accepted, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% Lease a container
handle_call({lease_container, Name, Mode, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            Parameters = [{comp, lease},
                          {res_type, container},
                          {lease_action, Mode}],

            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Name,
                                                    Parameters,
                                                    Options),

            {?http_accepted, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% List blobs
handle_call({list_blobs, Name, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            Parameters = [{comp, list},
                          {res_type, container}],

            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    Name,
                                                    Parameters,
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {ok, Blobs} = erlazure_blob:parse_blob_list(Body),
            {reply, Blobs, State};

% Put block blob
handle_call({put_blob, Container, Name, Type = block_blob, Data, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContextBase = create_request_context(?blob_service,
                                                        State,
                                                        put,
                                                        Container ++ "/" ++ Name,
                                                        Data,
                                                        [{blob_type, Type}],
                                                        Options),

            RequestContext = RequestContextBase#req_context{content_type = "application/octet-stream"},

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Put page blob
handle_call({put_blob, Container, Name, Type = page_blob, ContentLength, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),

            Parameters = [{blob_type, Type},
                          {blob_content_length, ContentLength}],

            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Name,
                                                    Parameters,
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Get blob
handle_call({get_blob, Container, Blob, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    Container ++ "/" ++ Blob,
                                                    [],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, Body}, State};

% Snapshot blob
handle_call({snapshot_blob, Container, Blob, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Blob,
                                                    [{comp, snapshot}],
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Copy blob
handle_call({copy_blob, Container, Blob, Source, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Blob,
                                                    [{blob_copy_source, Source}],
                                                    Options),

            {?http_accepted, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Delete blob
handle_call({delete_blob, Container, Blob, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    delete,
                                                    Container ++ "/" ++ Blob,
                                                    [],
                                                    Options),

            {?http_accepted, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, State};

% Put block
handle_call({put_block, Container, Blob, BlockId, Content, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),

            Parameters = [{comp, block},
                          {blob_block_id, base64:encode_to_string(BlockId)}],

            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Blob,
                                                    Content,
                                                    Parameters,
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Put block list
handle_call({put_block_list, Container, Blob, BlockRefs, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Blob,
                                                    erlazure_blob:get_request_body(BlockRefs),
                                                    [{comp, "blocklist"}],
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, created}, State};

% Get block list
handle_call({get_block_list, Container, Blob, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),
            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    Container ++ "/" ++ Blob,
                                                    [{comp, "blocklist"}],
                                                    Options),

            {?http_ok, Body} = execute_request(ServiceContext, RequestContext),
            {ok, {"BlockList", _, Elements}, _} = erlsom:simple_form(Body),
            {reply, erlazure_blob:parse_block_list(Elements), State};

% Acquire blob lease
handle_call({acquire_blob_lease, Container, Blob, ProposedId, Duration, Options}, _From, State) ->
            ServiceContext = create_service_context(?blob_service, State),

            Parameters = [{lease_action, acquire},
                          {proposed_lease_id, ProposedId},
                          {lease_duration, Duration},
                          {comp, lease}],

            RequestContext = create_request_context(?blob_service,
                                                    State,
                                                    put,
                                                    Container ++ "/" ++ Blob,
                                                    Parameters,
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, acquired}, State};

% List tables
handle_call({get_table_list, Options}, _From, State) ->
            ServiceContext = create_service_context(?table_service, State),
            Parameters = [],
            RequestContext = create_request_context(?table_service,
                                                    State,
                                                    get,
                                                    "Tables",
                                                    Parameters,
                                                    Options),

            {?http_created, _Body} = execute_request(ServiceContext, RequestContext),
            {reply, {ok, acquired}, State}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVer, State, _Extra) -> {ok, State}.

%%--------------------------------------------------------------------
%% Private functions
%%--------------------------------------------------------------------

execute_request(ServiceContext = #service_context{}, RequestContext = #req_context{}) ->
                Headers =  [{"x-ms-date", httpd_util:rfc1123_date()},
                            {"x-ms-version", ServiceContext#service_context.api_version},
                            {"Content-Type", RequestContext#req_context.content_type},
                            {"Content-Length", integer_to_list(RequestContext#req_context.content_length)},
                            {"Host", get_host(ServiceContext#service_context.service,
                                              ServiceContext#service_context.account)}]
                            ++ RequestContext#req_context.headers,

                AuthHeader = {"Authorization", get_shared_key(ServiceContext#service_context.service,
                                                              ServiceContext#service_context.account,
                                                              ServiceContext#service_context.key,
                                                              RequestContext#req_context.method,
                                                              RequestContext#req_context.path,
                                                              RequestContext#req_context.parameters,
                                                              Headers)},

                %% Fiddler
                %% httpc:set_options([{ proxy, {{"localhost", 9999}, []}}]),

                Response = httpc:request(RequestContext#req_context.method,
                                         erlazure_http:create_request(RequestContext, [AuthHeader | Headers]),
                                         [{version, "HTTP/1.1"}],
                                         [{sync, true}, {body_format, binary}, {headers_as_is, true}]),
                case Response of
                  {ok, {{_, Code, _}, _, Body}}
                  when Code >= 200, Code =< 204 ->
                       {Code, Body};

                  {ok, {{_, _, _}, _, Body}} ->
                       throw(Body)
                end.

get_shared_key(Service, Account, Key, HttpMethod, Path, Parameters, Headers) ->
                SignatureString = erlazure_http:verb_to_str(HttpMethod) ++ "\n" ++
                                  get_headers_string(Service, Headers) ++
                                  canonicalize_headers(Headers) ++
                                  canonicalize_resource(Account, Path, Parameters),

                "SharedKey " ++ Account ++ ":" ++ base64:encode_to_string(sign_string(Key, SignatureString)).

get_headers_string(Service, Headers) ->
                FoldFun = fun(HeaderName, Acc) ->
                  case lists:keyfind(HeaderName, 1, Headers) of
                    {HeaderName, Value} -> Acc ++ Value ++ "\n";
                    false -> Acc ++ "\n"
                  end
                end,
                lists:foldl(FoldFun, "", get_header_names(Service)).

sign_string(Key, StringToSign) ->
                crypto:hmac(sha256, base64:decode(Key), StringToSign).

build_uri_base(Service, Account) ->
                "https://" ++ get_host(Service, Account) ++ "/".

get_host(Service, Account) ->
                Account ++ "." ++ erlang:atom_to_list(Service) ++ ".core.windows.net".

canonicalize_headers(Headers) ->
                MS_Header_Names = [HeaderName || {HeaderName, _} <- Headers, string:str(HeaderName, "x-ms-") =:= 1],
                Sorted_MS_Header_Names = lists:sort(MS_Header_Names),
                FoldFun = fun(HeaderName, Acc) ->
                  {_, Value} = lists:keyfind(HeaderName, 1, Headers),
                  Acc ++ HeaderName ++ ":" ++ Value ++ "\n"
                end,
                lists:foldl(FoldFun, "", Sorted_MS_Header_Names).

canonicalize_resource(Account, Path, []) ->
                "/" ++ Account ++ "/" ++ Path;

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

create_service_context(?queue_service, State=#state{}) ->
                #service_context{service = ?queue_service,
                                 api_version = ?queue_service_ver,
                                 account = State#state.account,
                                 key = State#state.key};

create_service_context(?blob_service, State=#state{}) ->
                #service_context{service = ?blob_service,
                                 api_version = ?blob_service_ver,
                                 account = State#state.account,
                                 key = State#state.key};

create_service_context(?table_service, State=#state{}) ->
                #service_context{service = ?table_service,
                                 api_version = ?table_service_ver,
                                 account = State#state.account,
                                 key = State#state.key}.

create_request_context(Service, State=#state{}, Parameters, Options) ->
                create_request_context(Service, State, "", Parameters, Options).

create_request_context(Service, State=#state{}, Path, Parameters, Options) ->
                create_request_context(Service, State, get, Path, Parameters, Options).

create_request_context(Service, State=#state{}, Method, Path, Parameters, Options) ->
                create_request_context(Service, State, Method, Path, [], Parameters, Options).

create_request_context(Service, State=#state{}, Method, Path, Body, Parameters, Options) ->
                ParameterCombinedList = Parameters ++ Options,
                RequestParameters = get_request_uri_params(ParameterCombinedList, State#state.param_specs),
                RequestHeaders = get_request_headers(ParameterCombinedList, State#state.param_specs),

                #req_context{address = build_uri_base(Service, State#state.account),
                             path = Path,
                             method = Method,
                             body = Body,
                             content_length = erlazure_http:get_content_length(Body),
                             parameters = RequestParameters,
                             headers = RequestHeaders}.

get_request_headers(Params, ParamSpecs) ->
                get_request_params(Params, ParamSpecs, header).

get_request_uri_params(Params, ParamSpecs) ->
                get_request_params(Params, ParamSpecs, uri).

get_request_params(Params, ParamSpecs, Type) ->
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

get_request_param_specs() ->
                ProcessFun = fun(Spec=#param_spec{}, Dictionary) ->
                                orddict:store(Spec#param_spec.id, Spec, Dictionary)
                            end,

                CommonParamSpecs = lists:foldl(ProcessFun, orddict:new(), get_request_common_param_specs()),
                BlobParamSpecs = lists:foldl(ProcessFun, CommonParamSpecs, erlazure_blob:get_request_param_specs()),

                lists:foldl(ProcessFun, BlobParamSpecs, erlazure_queue:get_request_param_specs()).

get_request_common_param_specs() ->
                [#param_spec{ id = comp, type = uri, name = "comp" },
                 #param_spec{ id = timeout, type = uri, name = "timeout" },
                 #param_spec{ id = max_results, type = uri, name = "maxresults" },
                 #param_spec{ id = prefix, type = uri, name = "prefix" },
                 #param_spec{ id = marker, type = uri, name = "marker" }].