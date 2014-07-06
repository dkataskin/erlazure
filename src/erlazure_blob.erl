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
%%% Blob REST service response parsers.
%%% ====================================================================

-module(erlazure_blob).
-author("Dmitry Kataskin").

-include("erlazure.hrl").

%% API
-export([parse_container_list/1, parse_blob_list/1, get_request_body/1, parse_block_list/1,
         get_request_param_specs/0, parse_blob_response/1]).

-spec parse_container_list(string()) -> {error, bad_response} | {[blob_container()], list()}.
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
            'Snapshot' -> Blob#cloud_blob { snapshot = erlazure_xml:parse_str(Elem) };
            'Url' -> Blob#cloud_blob { url = erlazure_xml:parse_str(Elem) };
            'Metadata' -> Blob#cloud_blob { metadata = erlazure_xml:parse_metadata(Elem) };
            'Properties' -> Blob#cloud_blob { properties = lists:reverse(parse_blob_properties(Elem)) };
            _ -> Blob
          end
        end,
        lists:foldl(FoldFun, #cloud_blob{}, Nodes).

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
            'LeaseDuration' -> [{lease_duration, erlang:list_to_atom(erlazure_xml:parse_str(Elem))} | Properties];
            'CopyId' -> [{copy_id, erlazure_xml:parse_str(Elem)} | Properties];
            'CopyStatus' ->
              Status = string:to_lower(erlazure_xml:parse_str(Elem)),
              [{copy_status, str_to_copy_status(Status)} | Properties];
            'CopySource' -> [{copy_source, erlazure_xml:parse_str(Elem)} | Properties];
            'CopyProgress' -> [{copy_progress, erlazure_xml:parse_str(Elem)} | Properties];
            'CopyCompletionTime' -> [{copy_completion_time, erlazure_xml:parse_str(Elem)} | Properties];
            'CopyStatusDescription' -> [{copy_status_description, erlazure_xml:parse_str(Elem)} | Properties];
            _ -> Properties
          end
        end,
        lists:foldl(FoldFun, [], Nodes).

parse_block_list(BlockListResponse) when is_binary(BlockListResponse) ->
        parse_block_list(erlang:binary_to_list(BlockListResponse));

parse_block_list(BlockListResponse) when is_list(BlockListResponse) ->
        {ParseResult, _} = xmerl_scan:string(BlockListResponse),
        case ParseResult#xmlElement.name of
          'BlockList' ->
            Nodes = erlazure_xml:filter_elements(ParseResult#xmlElement.content),
            FoldFun = fun(#xmlElement{ name = Name, content = Content }, {Committed, Uncommitted}) ->
                        case Name of
                          'CommittedBlocks' ->
                            Nodes1 = erlazure_xml:filter_elements(Content),
                            {lists:map(fun(Elem) -> parse_block(Elem, committed) end, Nodes1), Uncommitted};

                          'UncommittedBlocks' ->
                            Nodes1 = erlazure_xml:filter_elements(Content),
                            {Committed, lists:map(fun(Elem) -> parse_block(Elem, uncommitted) end, Nodes1)};

                          _ ->
                            {Committed, Uncommitted}
                        end
                      end,
            {ok, lists:foldl(FoldFun, {[], []}, Nodes)};

          _ -> {error, bad_response}
        end.

parse_block(#xmlElement{ content = Content }, Type) ->
        Nodes = erlazure_xml:filter_elements(Content),
        FoldFun = fun(Elem=#xmlElement{ name = Name1 }, Block=#blob_block{}) ->
                    case Name1 of
                      'Name' ->
                          Block#blob_block { id = base64:decode_to_string(erlazure_xml:parse_str(Elem)) };

                      'Size' ->
                          Block#blob_block { size = erlazure_xml:parse_int(Elem) };

                      _ ->
                          Block
                    end
                  end,
        lists:foldl(FoldFun, #blob_block{ type = Type }, Nodes).

str_to_blob_type("BlockBlob") -> block_blob;
str_to_blob_type("PageBlob") -> page_blob.

block_type_to_node(uncommitted) -> 'Uncommitted';
block_type_to_node(committed) -> 'Committed';
block_type_to_node(latest) -> 'Latest'.

str_to_copy_status("aborted") -> aborted;
str_to_copy_status("failed") -> failed;
str_to_copy_status("invalid") -> invalid;
str_to_copy_status("pending") -> pending;
str_to_copy_status("success") -> success;
str_to_copy_status(_) -> unknown.

get_request_body(BlockRefs) ->
        FoldFun = fun(BlockRef=#blob_block{}, Acc) ->
                      [{block_type_to_node(BlockRef#blob_block.type),
                        [],
                        [base64:encode_to_string(BlockRef#blob_block.id)]} | Acc]
                  end,
        Data = {'BlockList', [], lists:reverse(lists:foldl(FoldFun, [], BlockRefs))},
        lists:flatten(xmerl:export_simple([Data], xmerl_xml)).

get_request_param_specs() ->
        [#param_spec{ id = block_list_type, type = uri, name = "blocklisttype" },
         #param_spec{ id = blob_block_id, type = uri, name = "blockid" },
         #param_spec{ id = res_type, type = uri, name = "restype" },
         #param_spec{ id = blob_copy_source, type = header, name = "x-ms-copy-source" },
         #param_spec{ id = blob_type, type = header, name = "x-ms-blob-type", parse_fun = fun erlang:atom_to_list/1 },
         #param_spec{ id = blob_content_length, type = header, name = "x-ms-blob-content-length" },
         #param_spec{ id = proposed_lease_id, type = header, name = "x-ms-proposed-lease-id" },
         #param_spec{ id = lease_id, type = header, name = "x-ms-lease-id" },
         #param_spec{ id = lease_duration, type = header, name = "x-ms-lease-duration" },
         #param_spec{ id = lease_break_period, type = header, name = "x-ms-break-period" },
         #param_spec{ id = lease_action, type = header, name = "x-ms-lease-action" }].