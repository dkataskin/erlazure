{application, erlazure,
          [{description, "Windows Azure Erlang bindings"},
            {vsn, "1"},
            {modules, [erlazure, erlazure_queue, erlazure_blob, erlazure_xml, erlazure_http]},
            {registered, []},
            {applications, [kernel, stdlib, crypto, inets, ssl]},
            {mod, {erlazure, []}},
            {env, []}]}.
