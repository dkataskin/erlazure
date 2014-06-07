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

%%% ====================================================================
%%% Queue REST service response parsers.
%%% ====================================================================

-module(erlazure_queue).
-author("Dmitry Kataskin").

-include("erlazure.hrl").

%% API
-export([parse_queue_list/1, parse_queue_messages_list/1, get_request_body/1, get_request_param_specs/0,
         parse_queue_acl_response/1]).

parse_queue_messages_list(Response) when is_binary(Response) ->
          parse_queue_messages_list(erlang:binary_to_list(Response));

parse_queue_messages_list(Response) when is_list(Response) ->
          {ParseResult, _} = xmerl_scan:string(Response),
          case ParseResult#xmlElement.name of
            'QueueMessagesList' ->
              Nodes = erlazure_xml:filter_elements(ParseResult#xmlElement.content),
              {ok, lists:map(fun parse_queue_message/1, Nodes)};
            _ -> {error, bad_response}
          end.

parse_queue_message(Elem=#xmlElement{}) ->
          Nodes = erlazure_xml:filter_elements(Elem#xmlElement.content),
          lists:foldl(fun parse_queue_mesage/2, #queue_message{}, Nodes).

parse_queue_mesage(Elem=#xmlElement{}, Message=#queue_message{}) ->
          case Elem#xmlElement.name of
            'MessageId' -> Message#queue_message{ id = erlazure_xml:parse_str(Elem) };
            'InsertionTime' -> Message#queue_message { insertion_time = erlazure_xml:parse_str(Elem) };
            'ExpirationTime' -> Message#queue_message { exp_time = erlazure_xml:parse_str(Elem) };
            'PopReceipt' -> Message#queue_message { pop_receipt = erlazure_xml:parse_str(Elem) };
            'TimeNextVisible' -> Message#queue_message { next_visible = erlazure_xml:parse_str(Elem) };
            'DequeueCount' -> Message#queue_message { dequeue_count = erlazure_xml:parse_int(Elem) };
            'MessageText' -> Message#queue_message { text = base64:decode_to_string(erlazure_xml:parse_str(Elem)) }
          end.

parse_queue_list(Response) ->
          ParserSpec = #enum_parser_spec { rootKey = 'Queues',
                                           elementKey = 'Queue',
                                           elementParser = fun parse_queue_response/1 },
          erlazure_xml:parse_enumeration(Response, ParserSpec).

parse_queue_response(#xmlElement { content = Content }) ->
          Nodes = erlazure_xml:filter_elements(Content),
          FoldFun = fun(Elem=#xmlElement{}, Queue=#queue{}) ->
            case Elem#xmlElement.name of
              'Name' -> Queue#queue { name = erlazure_xml:parse_str(Elem) };
              'Url' -> Queue#queue { url = erlazure_xml:parse_str(Elem) };
              'Metadata' -> Queue#queue { metadata = erlazure_xml:parse_metadata(Elem) };
              _ -> Queue
            end
          end,
          lists:foldl(FoldFun, #queue{}, Nodes).

parse_queue_acl_response(Response) when is_binary(Response) ->
          parse_queue_acl_response(binary_to_list(Response));

parse_queue_acl_response(Response) when is_list(Response) ->
          ?PRINT(Response),
          {ParseResult, _} = xmerl_scan:string(Response),
          case ParseResult#xmlElement.name of
            'SignedIdentifiers' ->
              case lists:keyfind('SignedIdentifier', 2, ParseResult#xmlElement.content) of
                false -> {ok, no_acl};
                SignedIdNode ->
                  Nodes = erlazure_xml:filter_elements(SignedIdNode#xmlElement.content),
                  FoldFun = fun(Elem=#xmlElement{}, SignedId=#signed_id{}) ->
                              case Elem#xmlElement.name of
                                'Id' -> SignedId#signed_id { id = base64:decode_to_string(erlazure_xml:parse_str(Elem)) };
                                'AccessPolicy' -> SignedId#signed_id { access_policy = parse_access_policy(Elem) };
                                _ -> SignedId
                              end
                            end,
                  {ok, lists:foldl(FoldFun, #signed_id{}, Nodes)}
              end;
            _ ->
              {error, bad_response}
          end.

parse_access_policy(XmlElement=#xmlElement{}) ->
          Nodes = erlazure_xml:filter_elements(XmlElement#xmlElement.content),
          FoldFun = fun(Elem=#xmlElement{}, AccessPolicy=#access_policy{}) ->
                      case Elem#xmlElement.name of
                        'Start' -> AccessPolicy#access_policy { start = erlazure_xml:parse_str(Elem) };
                        'Expiry' -> AccessPolicy#access_policy { expiry = erlazure_xml:parse_str(Elem) };
                        'Permission' -> AccessPolicy#access_policy { permission = erlazure_xml:parse_str(Elem) };
                        _ -> AccessPolicy
                      end
                    end,
          lists:foldl(FoldFun, #access_policy{}, Nodes).

get_request_body(Message) ->
          Data = {'QueueMessage', [], [{'MessageText', [], [base64:encode_to_string(Message)]}]},
          lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

-spec get_request_param_specs() -> list(param_spec()).
get_request_param_specs() ->
          [#param_spec{ id = num_of_messages, type = uri, name = "numofmessages" },
           #param_spec{ id = pop_receipt, type = uri, name = "popreceipt" },
           #param_spec{ id = peek_only, type = uri, name = "peekonly" },
           #param_spec{ id = message_ttl, type = uri, name = "messagettl" },
           #param_spec{ id = message_visibility_timeout, type = uri, name = "visibilitytimeout" }].