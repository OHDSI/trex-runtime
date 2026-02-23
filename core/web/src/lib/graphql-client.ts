import { type ReactNode, createElement } from "react";
import { Client, Provider, cacheExchange, fetchExchange, subscriptionExchange } from "urql";
import { createClient as createWSClient } from "graphql-ws";
import { BASE_PATH } from "./config";

const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
const wsClient = createWSClient({
  url: `${wsProtocol}//${window.location.host}${BASE_PATH}/graphql`,
});

export const client = new Client({
  url: `${BASE_PATH}/graphql`,
  exchanges: [
    cacheExchange,
    subscriptionExchange({
      forwardSubscription(request) {
        const input = { ...request, query: request.query || "" };
        return {
          subscribe(sink) {
            const unsubscribe = wsClient.subscribe(input, sink);
            return { unsubscribe };
          },
        };
      },
    }),
    fetchExchange,
  ],
  fetchOptions: () => ({
    credentials: "include" as const,
    method: "POST",
  }),
  preferGetMethod: false,
});

export function GraphQLProvider({ children }: { children: ReactNode }) {
  return createElement(Provider, { value: client }, children);
}
