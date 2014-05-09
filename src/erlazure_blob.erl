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

%%% ====================================================================
%%% Blob REST service response parsers.
%%% ====================================================================

-module(erlazure_blob).
-author("Dmitry Kataskin").

-include_lib("xmerl/include/xmerl.hrl").
-include("erlazure.hrl").


%% API
-export([parse_container_list/1, parse_blob_list/1, parse_blob/1, get_request_body/1, parse_block_list/1,
         get_request_param_specs/0]).

parse_container_list(Response) ->
                ParserSpec = #enum_parser_spec { rootKey = 'Containers',
                                                 elementKey = 'Container',
                                                 elementParser = fun parse_container_response/1 },
                erlazure_xml:parse_enumeration(Response, ParserSpec).

parse_blob_list(Response) ->
                ParserSpec = #enum_parser_spec { rootKey = 'Blobs',
                                                 elementKey = 'Blob',
                                                 elementParser = fun parse_blob_response/1 },
                erlazure_xml:parse_enumeration(Response, ParserSpec).

parse_container_response(#xmlElement { content = Content }) ->
                Nodes = erlazure_xml:filter_elements(Content),
                FoldFun = fun(Elem=#xmlElement{}, Container=#blob_container{}) ->
                  case Elem#xmlElement.name of
                    'Name' -> Container#blob_container { name = erlazure_xml:parse_str(Elem) };
                    'Url' -> Container#blob_container { url = erlazure_xml:parse_str(Elem) };
                    'Metadata' -> Container#blob_container { metadata = erlazure_xml:parse_metadata(Elem) };
                    'Properties' -> Container#blob_container { properties = parse_container_properties(Elem) };
                    _ -> Container
                  end
                end,
                lists:foldl(FoldFun, #blob_container{}, Nodes).

parse_container_properties(#xmlElement { content = Content }) ->
                Nodes = erlazure_xml:filter_elements(Content),
                FoldFun = fun(Elem=#xmlElement{}, Properties) ->
                  case Elem#xmlElement.name of
                    'Last-Modified' -> [{last_modified, erlazure_xml:parse_str(Elem)} | Properties];
                    'Etag' -> [{etag, erlazure_xml:parse_str(Elem)} | Properties];
                    'LeaseStatus' -> [{lease_status, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
                    'LeaseState' -> [{lease_state, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
                    'LeaseDuration' -> [{lease_duration, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
                    _ -> Properties
                  end
                end,
                lists:reverse(lists:foldl(FoldFun, [], Nodes)).

parse_blob_response(#xmlElement { content = Content }) ->
                Nodes = erlazure_xml:filter_elements(Content),
                FoldFun = fun(Elem=#xmlElement{}, Blob=#cloud_blob{}) ->
                  case Elem#xmlElement.name of
                    'Name' -> Blob#cloud_blob { name = erlazure_xml:parse_str(Elem) };
                    'Url' -> Blob#cloud_blob { url = erlazure_xml:parse_str(Elem) };
                    'Metadata' -> Blob#cloud_blob { metadata = erlazure_xml:parse_metadata(Elem) };
                    'Properties' -> Blob#cloud_blob { properties = parse_blob_properties(Elem) };
                    _ -> Blob
                  end
                end,
                lists:foldl(FoldFun, #blob_container{}, Nodes).

parse_blob_properties(#xmlElement { content = Content }) ->
                Nodes = erlazure_xml:filter_elements(Content),
                FoldFun = fun(Elem=#xmlElement{}, Properties) ->
                  case Elem#xmlElement.name of
                    'Last-Modified' -> [{last_modified, erlazure_xml:parse_str(Elem)} | Properties];
                    'Etag' -> [{etag, erlazure_xml:parse_str(Elem)} | Properties];
                    'Content-Length' -> [{content_length, erlazure_xml:parse_int(Elem)} | Properties];
                    'Content-Type' -> [{content_type, erlazure_xml:parse_str(Elem)} | Properties];
                    'Content-Encoding' -> [{content_encoding, erlazure_xml:parse_str(Elem)} | Properties];
                    'Content-Language' -> [{content_language, erlazure_xml:parse_str(Elem)} | Properties];
                    'Content-MD5' -> [{content_md5, erlazure_xml:parse_str(Elem)} | Properties];
                    'Cache-Control' -> [{cache_control, erlazure_xml:parse_str(Elem)} | Properties];
                    'x-ms-blob-sequence-number' -> [{sequence_number, erlazure_xml:parse_str(Elem)} | Properties];
                    'BlobType' -> [{blob_type, str_to_blob_type(erlazure_xml:parse_str(Elem))} | Properties];
                    'LeaseStatus' -> [{lease_status, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
                    'LeaseState' -> [{lease_state, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
                    'LeaseDuration' -> [{lease_duration, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties]
                  end
                end,
                lists:foldl(FoldFun, [], Nodes).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
parse_block_list(Blocks) ->
                io:format("~p~n", [Blocks]),
                Committed = parse_block_list("CommittedBlocks", committed, Blocks),
                Uncommitted = parse_block_list("UncommittedBlocks", uncommitted, Blocks),
                Committed ++ Uncommitted.

parse_block_list(NodeName, BlockType, Blocks) ->
                case lists:keyfind(NodeName, 1, Blocks) of
                  {NodeName, _, Elements} ->
                    ParseFun = fun(Element) -> parse_block(Element, BlockType) end,
                    erlazure_xml:parse_list(ParseFun, Elements);
                  false -> []
                end.

parse_blob({"Blob", _, Elements}) ->
                {"Properties", _, Properties} = lists:keyfind("Properties", 1, Elements),

                #cloud_blob{
                  name = erlazure_xml:get_element_text("Name", Elements),
                  url = erlazure_xml:get_element_text("Url", Elements)%,
                  %last_modified = erlazure_xml:get_element_text("Last-Modified", Properties),
                  %etag = erlazure_xml:get_element_text("ETag", Properties),
                  %content_length = list_to_integer(erlazure_xml:get_element_text("Content-Length", Properties)),
                  %content_type = erlazure_xml:get_element_text("Content-Type", Properties),
                  %content_encoding = erlazure_xml:get_element_text("Content-Encoding", Properties),
                  %content_language = erlazure_xml:get_element_text("Content-Language", Properties),
                  %content_md5 = erlazure_xml:get_element_text("Content-MD5", Properties),
                  %cache_control = erlazure_xml:get_element_text("Cache-Control", Properties),
                  %type = str_to_blob_type(erlazure_xml:get_element_text("BlobType", Properties)),
                  %copy = parse_copy_state(Properties),
                  %metadata = erlazure_xml:parse_metadata(Elements)
                }.

parse_block({"Block", _, Elements}, Type) ->
                #blob_block{
                  id = base64:decode_to_string(erlazure_xml:get_element_text("Name", Elements)),
                  size = list_to_integer(erlazure_xml:get_element_text("Size", Elements)),
                  type = Type
                }.

parse_copy_state(Properties) ->
                case lists:keyfind("CopyId", 1, Properties) of
                  {"CopyId", _, _} ->
                    #blob_copy_state{
                      id = erlazure_xml:get_element_text("CopyId", Properties),
                      status = list_to_atom(erlazure_xml:get_element_text("CopyStatus", Properties)),
                      source = erlazure_xml:get_element_text("CopySource", Properties),
                      progress = erlazure_xml:get_element_text("CopyProgress", Properties),
                      completion_time = erlazure_xml:get_element_text("CopyCompletionTime", Properties),
                      status_description = erlazure_xml:get_element_text("CopyStatusDescription", Properties)
                    };
                false ->
                  undefined
                end.

str_to_blob_type("BlockBlob") -> block_blob;
str_to_blob_type("PageBlob") -> page_blob.

blob_type_to_str(block_blob) -> "BlockBlob";
blob_type_to_str(page_blob) -> "PageBlob".

str_to_block_type("Uncommitted") -> uncommitted;
str_to_block_type("Committed") -> committed;
str_to_block_type("Latest") -> latest.

block_type_to_str(uncommitted) -> "Uncommitted";
block_type_to_str(committed) -> "Committed";
block_type_to_str(latest) -> "Latest".

block_type_to_node(uncommitted) -> 'Uncommitted';
block_type_to_node(committed) -> 'Committed';
block_type_to_node(latest) -> 'Latest'.

get_request_body(BlockRefs) ->
                FoldFun = fun(BlockRef=#blob_block_ref{}, Acc) ->
                              [{block_type_to_node(BlockRef#blob_block_ref.type),
                                [],
                                [base64:encode_to_string(BlockRef#blob_block_ref.id)]} | Acc]
                          end,
                Data = {'BlockList', [], lists:reverse(lists:foldl(FoldFun, [], BlockRefs))},
                lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

parse_lease_properties(Elem=#xmlElement{}) ->
                erlang:list_to_atom(erlazure_xml:parse_str(Elem)).

get_blob_property_specs() ->
                [#property_spec{ name = last_modified, key = 'Last-Modified' },
                 #property_spec{ name = etag, key = 'Etag' },
                 #property_spec{ name = content_length, key = 'Content-Length', parse_fun = fun erlazure_xml:parse_int/1 },
                 #property_spec{ name = content_type, key = 'Content-Type' },
                 #property_spec{ name = content_encoding, key = 'Content-Encoding' },
                 #property_spec{ name = content_language, key = 'Content-Language' },
                 #property_spec{ name = content_md5, key = 'Content-MD5' },
                 #property_spec{ name = cache_control, key = 'Cache-Control' },
                 #property_spec{ name = sequence_number, key = 'x-ms-blob-sequence-number' },
                 #property_spec{ name = blob_type, key = 'BlobType', parse_fun = fun(Elem=#xmlElement{}) -> str_to_blob_type(erlazure_xml:parse_str(Elem)) end },
                 #property_spec{ name = lease_status, key = 'LeaseStatus', parse_fun = fun parse_lease_properties/1 },
                 #property_spec{ name = lease_state, key = 'LeaseState', parse_fun = fun parse_lease_properties/1 },
                 #property_spec{ name = lease_duration, key = 'LeaseDuration', parse_fun = fun parse_lease_properties/1 }].

get_request_param_specs() ->
                [#param_spec{ id = block_list_type, type = uri, name = "blocklisttype" },
                 #param_spec{ id = blob_block_id, type = uri, name = "blockid" },
                 #param_spec{ id = res_type, type = uri, name = "restype" },
                 #param_spec{ id = blob_copy_source, type = header, name = "x-ms-copy-source" },
                 #param_spec{ id = blob_type, type = header, name = "x-ms-blob-type", parse_fun = fun erlazure_blob:blob_type_to_str/1 },
                 #param_spec{ id = blob_content_length, type = header, name = "x-ms-blob-content-length" },
                 #param_spec{ id = proposed_lease_id, type = header, name = "x-ms-proposed-lease-id" },
                 #param_spec{ id = lease_id, type = header, name = "x-ms-lease-id" },
                 #param_spec{ id = lease_duration, type = header, name = "x-ms-lease-duration" },
                 #param_spec{ id = lease_break_period, type = header, name = "x-ms-break-period" },
                 #param_spec{ id = lease_action, type = header, name = "x-ms-lease-action" }].