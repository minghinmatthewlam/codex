import SwiftUI

@main
struct CodexRemoteApp: App {
    @StateObject private var session = RemoteSession()

    var body: some Scene {
        WindowGroup {
            ContentView(session: session)
                .task {
                    await session.connectFromLaunchArgumentsIfPresent()
                }
        }
    }
}
