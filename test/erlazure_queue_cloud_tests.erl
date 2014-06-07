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

-module(erlazure_queue_cloud_tests).
-author("Dmitry Kataskin").

-compile(export_all).

-define(DEBUG, true).

-include("erlazure.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

create_queue_test_() ->
                {setup,
                 fun start/0,
                 fun stop/1,
                 fun create_queue/1}.

create_queue_duplicate_name_test_() ->
                {setup,
                 fun start_create/0,
                 fun stop/1,
                 fun create_queue_duplicate_name/1}.

list_queues_test_() ->
                {setup,
                 fun start_create/0,
                 fun stop/1,
                 fun list_queues/1}.

delete_queue_test_() ->
                {setup,
                 fun start_create/0,
                 fun stop/1,
                 fun delete_queue/1}.

delete_queue_twice_test_() ->
                {setup,
                 fun start_create/0,
                 fun stop/1,
                 fun delete_queue_twice/1}.

get_queue_acl_test_() ->
                {setup,
                  fun start_create/0,
                  fun stop/1,
                  fun get_queue_acl/1}.

put_message_test_() ->
                {setup,
                 fun start_create/0,
                 fun stop/1,
                 fun put_message/1}.

start() ->
    {ok, Pid} = erlazure:start(?account_name, ?account_key),
    UniqueQueueName = get_queue_unique_name(),
    {Pid, UniqueQueueName}.

start_create() ->
                {ok, Pid} = erlazure:start(?account_name, ?account_key),
                UniqueQueueName = get_queue_unique_name(),
                {ok, created} = erlazure:create_queue(Pid, UniqueQueueName),
                {Pid, UniqueQueueName}.

stop({Pid, QueueName}) ->
                erlazure:delete_queue(Pid, QueueName).

create_queue({Pid, QueueName}) ->
                Response = erlazure:create_queue(Pid, QueueName),
                ?_assertMatch({ok, created}, Response).

create_queue_duplicate_name({Pid, QueueName}) ->
                Response = erlazure:create_queue(Pid, QueueName),
                ?_assertMatch({error, already_created}, Response).

list_queues({Pid, QueueName}) ->
                LowerQueueName = string:to_lower(QueueName),
                {ok, {Queues, _Metadata}} = erlazure:list_queues(Pid),
                Queue = lists:keyfind(LowerQueueName, 2, Queues),
                ?_assertMatch(#queue { name = LowerQueueName }, Queue).

delete_queue({Pid, QueueName}) ->
                Response = erlazure:delete_queue(Pid, QueueName),
                ?_assertMatch({ok, deleted}, Response).

delete_queue_twice({Pid, QueueName}) ->
                {ok, deleted} = erlazure:delete_queue(Pid, QueueName),
                Response = erlazure:delete_queue(Pid, QueueName),
                ?_assertMatch({ok, deleted}, Response).

get_queue_acl({Pid, QueueName}) ->
                Response = erlazure:get_queue_acl(Pid, QueueName),
                ?_assertMatch({ok, no_acl}, Response).

put_message({Pid, QueueName}) ->
                Response = erlazure:put_message(Pid, QueueName, "test message"),
                ?_assertMatch({ok, created}, Response).

get_queue_unique_name() ->
                test_utils:append_ticks("TestQueue").