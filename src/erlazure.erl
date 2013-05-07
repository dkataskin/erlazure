%%% Copyright (C) 2013 Dmitriy Kataskin
%%%
%%% This file is part of ErlAzure.
%%%
%%% ErlAzure is free software: you can redistribute it and/or modify
%%% it under the terms of the GNU Lesser General Public License as
%%% published by the Free Software Foundation, either version 3 of
%%% the License, or (at your option) any later version.
%%%
%%% ErlAzure is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
%%% GNU Lesser General Public License for more details.
%%%
%%% You should have received a copy of the GNU Lesser General Public
%%% License along with ErlAzure.  If not, see
%%% <http://www.gnu.org/licenses/>.
%%%
%%% Author contact: dmitriy.kataskin@gmail.com

%%% ====================================================================
%%% Azure Storage API.
%%% ====================================================================

-module(erlazure).
-author("Dmitriy Kataskin").

-define(http_ok, 200).
-define(http_created, 201).
-define(http_accepted, 202).
-define(http_no_content, 204).

-define(blob_service, blob).
-define(table_service, table).
-define(queue_service, queue).

-define(queue_service_ver, "2012-02-12").

-include("..\\include\\erlazure.hrl").

-record(service_context, {service, api_version, account, key}).
-record(request_context, {method = get,
                          address,
                          path = "",
                          parameters = [],
                          content_type = "application/xml",
                          body = "",
                          headers = []}).

-behaviour(gen_server).

%% API
-export([start/2, list_queues/0, get_queue_acl/1, get_queue_acl/2, create_queue/1, create_queue/2,
         delete_queue/1, delete_queue/2, put_message/2, put_message/3, get_messages/1, get_messages/2,
         peek_messages/1, peek_messages/2, delete_message/2, delete_message/3, clear_messages/1,
         clear_messages/2, update_message/3, update_message/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%%====================================================================
%% API
%%====================================================================

start(Account, Key) ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, {Account, Key}, []).

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

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Account, Key}) ->
            crypto:start(),
            inets:start(),
            ssl:start(),
            {ok, {Account, Key}}.

% List queues
handle_call({Action=list_queues, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),
            Parameters = [{"comp", "list"}],

            RequestContext = create_request_context(?queue_service, Account,
                                                    "",
                                                    Parameters ++ parse_queue_request_options(Action, Options)),

            {?http_ok, Body} = do_service_request(ServiceContext, RequestContext),
            {ok, {_, _, Elements}, _} = erlsom:simple_form(Body),

            case lists:keyfind("Queues", 1, Elements) of
              {"Queues", _, QueueListElement} ->
                  {reply, erlazure_queue:parse_queue_list(QueueListElement), {Account, Key}};
              false ->
                  {reply, [], {Account, Key}}
            end;

% Get queue acl
handle_call({Action=get_queue_acl, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),
            Parameters = [{"comp", "acl"}],

            RequestContext = create_request_context(?queue_service, Account,
                                                    string:to_lower(Queue),
                                                    Parameters ++ parse_queue_request_options(Action, Options)),

            {?http_ok, Body} = do_service_request(ServiceContext, RequestContext),
            {reply, Body, {Account, Key}};

% Create queue
handle_call({Action=create_queue, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    put,
                                                    string:to_lower(Queue),
                                                    parse_queue_request_options(Action, Options)),

            {?http_created, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, created}, {Account, Key}};

% Delete queue
handle_call({Action=delete_queue, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    delete,
                                                    string:to_lower(Queue),
                                                    parse_queue_request_options(Action, Options)),

            {?http_no_content, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, {Account, Key}};

% Add message to a queue
handle_call({Action=put_message, Queue, Message, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    post,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    parse_queue_request_options(Action, Options),
                                                    erlazure_queue:get_request_body(Message)),

            {?http_created, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, created}, {Account, Key}};

% Get messages from the queue
handle_call({Action=get_messages, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    parse_queue_request_options(Action, Options)),

            {?http_ok, Body} = do_service_request(ServiceContext, RequestContext),
            {ok, {_, _, Elements}, _} = erlsom:simple_form(Body),
            {reply, erlazure_queue:parse_message_list(Elements), {Account, Key}};

% Peek messages from the queue
handle_call({Action=peek_messages, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    [{"peekonly", "true"} | parse_queue_request_options(Action, Options)]),

            {?http_ok, Body} = do_service_request(ServiceContext, RequestContext),
            {ok, {_, _, Elements}, _} = erlsom:simple_form(Body),
            {reply, erlazure_queue:parse_message_list(Elements), {Account, Key}};

% Delete message from the queue
handle_call({Action=delete_message, Queue, MessageId, PopReceipt, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),
            Parameters = [{"popreceipt", PopReceipt}],

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    delete,
                                                    string:to_lower(Queue) ++ "/message/" ++ MessageId,
                                                    Parameters ++ parse_queue_request_options(Action, Options)),

            {?http_no_content, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, {Account, Key}};

% Delete all messages from the queue
handle_call({Action=clear_messages, Queue, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    delete,
                                                    string:to_lower(Queue) ++ "/messages",
                                                    parse_queue_request_options(Action, Options)),

            {?http_no_content, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, deleted}, {Account, Key}};

% Update a message in the queue
handle_call({Action=update_message, Queue, UpdatedMessage=#queue_message{}, VisibilityTimeout, Options}, _From, {Account, Key}) ->
            ServiceContext = create_service_context(?queue_service, Account, Key),

            Parameters = [{"popreceipt", UpdatedMessage#queue_message.pop_receipt},
                          {"visibilitytimeout", integer_to_list(VisibilityTimeout)}],

            RequestContext = create_request_context(?queue_service,
                                                    Account,
                                                    put,
                                                    string:to_lower(Queue) ++ "/messages/" ++ UpdatedMessage#queue_message.id,
                                                    Parameters ++ parse_queue_request_options(Action, Options),
                                                    erlazure_queue:get_request_body(UpdatedMessage#queue_message.text)),

            {?http_no_content, _Body} = do_service_request(ServiceContext, RequestContext),
            {reply, {ok, updated}, {Account, Key}}.

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVer, State, _Extra) -> {ok, State}.

%%--------------------------------------------------------------------
%%% Private functions
%%--------------------------------------------------------------------

do_service_request(ServiceContext = #service_context{}, RequestContext = #request_context{}) ->
            Headers =  [{"x-ms-date", httpd_util:rfc1123_date()},
                        {"x-ms-version", ServiceContext#service_context.api_version},
                        {"Content-Type", RequestContext#request_context.content_type},
                        {"Content-Length", integer_to_list(lists:flatlength(RequestContext#request_context.body))},
                        {"Host", get_host(ServiceContext#service_context.service,
                                          ServiceContext#service_context.account)}],

            AuthHeader = {"Authorization", get_shared_key(ServiceContext#service_context.service,
                                                          ServiceContext#service_context.account,
                                                          ServiceContext#service_context.key,
                                                          RequestContext#request_context.method,
                                                          RequestContext#request_context.path,
                                                          RequestContext#request_context.parameters,
                                                          Headers)},

            %% Fiddler
            %% httpc:set_options([{ proxy, {{"localhost", 9999}, []}}]),

            Response = httpc:request(RequestContext#request_context.method,
                                     create_request(RequestContext, [AuthHeader | Headers]),
                                     [{version, "HTTP/1.1"}],
                                     [{sync, true}, {body_format, binary}, {headers_as_is, true}]),
            case Response of
              {ok, {{_, Code, _}, _, Body}}
              when Code >= 200; Code =< 204 ->
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

combine_canonical_param({Param, Value}, _PreviosParam, Acc, ParamList) ->
            [H | T] = ParamList,
            combine_canonical_param(H, Param, add_param_value(Param, Value, Acc), T).

add_param_value(Param, Value, Acc) ->
            Acc ++ "\n" ++ string:to_lower(Param) ++ ":" ++ Value.

add_value(Value, Acc) ->
            Acc ++ "," ++ Value.

get_header_names(?blob_service) -> get_header_names(?queue_service);

get_header_names(?queue_service) -> ["Content-Encoding",
                                     "Content-Language",
                                     "Content-Length",
                                     "Content-MD5",
                                     "Content-Type",
                                     "Date",
                                     "If-Modified-Since",
                                     "If-Match",
                                     "If-None-Match",
                                     "If-Unmodified-Since",
                                     "Range"];

get_header_names(?table_service) -> ["Content-MD5",
                                     "Content-Type",
                                     "Date"].

construct_url(RequestContext = #request_context{}) ->
        FoldFun = fun({ParamName, ParamValue}, Acc) ->
                  if Acc =:= "" ->
                            "?" ++ ParamName ++ "=" ++ ParamValue;
                     true ->
                            Acc ++"&" ++ ParamName ++ "=" ++ ParamValue
                  end
        end,
        RequestContext#request_context.address ++
        RequestContext#request_context.path ++
        lists:foldl(FoldFun, "", RequestContext#request_context.parameters).

create_service_context(?queue_service, Account, Key) ->
              #service_context{service = ?queue_service,
                               api_version = ?queue_service_ver,
                               account = Account,
                               key = Key}.

create_request_context(Service, Account, Path) ->
              create_request_context(Service, Account, Path, []).

create_request_context(Service, Account, Path, Parameters) ->
              #request_context{address = build_uri_base(Service, Account),
                               path = Path,
                               parameters = Parameters}.

create_request_context(Service, Account, Method, Path, Parameters) ->
              #request_context{address = build_uri_base(Service, Account),
                               path = Path,
                               method = Method,
                               parameters = Parameters}.

create_request_context(Service, Account, Method, Path, Parameters, Body) ->
              #request_context{address = build_uri_base(Service, Account),
                               path = Path,
                               method = Method,
                               body = Body,
                               parameters = Parameters}.

parse_queue_request_options(_, []) -> [];

parse_queue_request_options(Action, Options) ->
              KnownOptions = erlazure_queue:get_request_options(Action),
              FoldFun = fun({Option, Value}, Acc) when is_atom(Option) ->
                          case lists:any(fun(OptionName) -> OptionName =:= Option end, KnownOptions) of
                              true when is_list(Value) -> [{atom_to_list(Option), Value} | Acc];
                              true -> [{atom_to_list(Option), lists:flatten(io_lib:format("~p", [Value]))} | Acc];
                              false -> throw("Unknown option " ++ atom_to_list(Option))
                          end
                        end,
              lists:foldl(FoldFun, [], Options).

create_request(RequestContext = #request_context{ method = get }, Headers) ->
        {construct_url(RequestContext), Headers};

create_request(RequestContext = #request_context{}, Headers) ->
        {construct_url(RequestContext),
        Headers,
        RequestContext#request_context.content_type,
        RequestContext#request_context.body}.