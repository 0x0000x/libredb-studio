import { editorExecutionContext } from "@/lib/api/execution-context";
import { scopeToCatalog } from "@/lib/db/catalog-scope";
import { getOrCreateProvider } from "@/lib/db";
import type { DatabaseProvider } from "@/lib/db/types";
import type { DatabaseConnection } from "@/lib/types";

/**
 * The provider an editor request runs on: the connection's cached provider, opened under the
 * session's editor posture, and on a server-level connection the session of `catalog` (#1530).
 *
 * The one place a route decides this, so no route opens a provider under another posture or skips
 * the catalog check. Routes that act on the whole connection rather than in one database, such as
 * cancel, which finds a statement in whichever session runs it, call `getOrCreateProvider` directly.
 */
export async function editorProvider(
  session: { role: string },
  connection: DatabaseConnection,
  catalog: string | undefined,
): Promise<DatabaseProvider> {
  const connected = await getOrCreateProvider(connection, {}, editorExecutionContext(session, connection));
  return scopeToCatalog(connected, catalog);
}
