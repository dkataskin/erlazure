-module(erlazure_ad_auth).

%% this module implements Azure Active Directory Authentication Protocols
%% which are described at https://msdn.microsoft.com/en-us/library/azure/dn151124.aspx

%% there is design decision to use Options proplist
%% for all the not required parameters
%% in order to shorten amount of functions with different number of arguments


%% This client is common to all tenants
%% This is used for username password logins
-define(CLIENT_ID, "04b07795-8ddb-461a-bbee-02f9e1bf7b46").
%% default resource
-define(RESOURCE, "https://management.core.windows.net/").

-export([
    oauth2_acquire_token_with_username_password/4
]).

-include("erlazure.hrl").

%% interface design questions:
%% support Config in erlcloud style to be able to change endpoints?
%%     also with possible retry mechanism

-spec oauth2_acquire_token_with_username_password(
    string(), string(), string(), proplists:proplist()) ->
    {ok, ad_oauth2_token()} | {error, Reason :: term()}.
oauth2_acquire_token_with_username_password(ADId, Username, Password, Options) ->
    Params = [
        {"grant_type", "password"},
        {"client_id", proplists:get_value(client_id, Options, ?CLIENT_ID)},
        {"resource", proplists:get_value(resource, Options, ?RESOURCE)},
        {"username", Username},
        {"password", Password}
    ],
    Url = "https://login.windows.net/" ++ ADId ++ "/oauth2/token",
    ReqBody = urlencode(Params),
    Response = httpc:request(post,
        {Url, [], "application/x-www-form-urlencoded", ReqBody},
        [], [{body_format, binary}]),
    case Response of
        {ok, {{_, 200, _}, _, Body}} ->
                {ok, to_oauth2_token(Body)};
        {ok, {{_, Code, _}, _, Body}} ->
            {error, {http_error, {Code, Body}}};
        {error, _} = Error ->
            Error
    end.

to_oauth2_token(Body) ->
    Json = jsx:decode(Body),
    #ad_oauth2_token{
        token_type = proplists:get_value(<<"token_type">>, Json),
        expires_in = proplists:get_value(<<"expires_in">>, Json),
        scope = proplists:get_value(<<"scope">>, Json),
        expires_on = proplists:get_value(<<"expires_on">>, Json),
        not_before = proplists:get_value(<<"not_before">>, Json),
        resource = proplists:get_value(<<"resource">>, Json),
        access_token = proplists:get_value(<<"access_token">>, Json),
        refresh_token = proplists:get_value(<<"refresh_token">>, Json)
    }.

urlencode(Params) ->
    Encoded = [ {http_uri:encode(K), http_uri:encode(V)} || {K, V} <- Params ],
    string:join([ K ++ "=" ++ V || {K, V} <- Encoded ], "&").
