import Foundation

@MainActor
final class RemoteSession: ObservableObject {
    enum ConnectionState: Equatable {
        case disconnected
        case connecting
        case connected
        case sending
        case forking
        case reconnecting
        case offline

        var title: String {
            switch self {
            case .disconnected: "Ready"
            case .connecting: "Connecting"
            case .connected: "Connected"
            case .sending: "Sending"
            case .forking: "Forking"
            case .reconnecting: "Reconnecting"
            case .offline: "Offline"
            }
        }
    }

    @Published var pairURL = ""
    @Published private(set) var state: ConnectionState = .disconnected
    @Published private(set) var snapshot = RemoteSnapshot.empty
    @Published var draft = ""
    @Published private(set) var forkCommand: String?
    @Published private(set) var errorMessage: String?

    private var baseURL: URL?
    private var token: String?
    private var eventsTask: Task<Void, Never>?

    deinit {
        eventsTask?.cancel()
    }

    var canSend: Bool {
        baseURL != nil && !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var isPaired: Bool {
        baseURL != nil
    }

    var canFork: Bool {
        snapshot.fork?.available == true && baseURL != nil
    }

    func connectFromLaunchArgumentsIfPresent() async {
        let args = ProcessInfo.processInfo.arguments
        guard let index = args.firstIndex(of: "--pair-url") else {
            return
        }
        let valueIndex = args.index(after: index)
        guard args.indices.contains(valueIndex) else {
            return
        }
        pairURL = args[valueIndex]
        await connect()
    }

    func connect() async {
        do {
            let endpoint = try RemoteEndpoint(rawPairURL: pairURL)
            baseURL = endpoint.baseURL
            token = endpoint.token
            eventsTask?.cancel()
            forkCommand = nil
            state = .connecting
            errorMessage = nil
            snapshot = try await fetchSnapshot(endpoint: endpoint)
            state = .connected
            startEventStream(endpoint: endpoint)
        } catch {
            baseURL = nil
            token = nil
            state = .offline
            errorMessage = error.localizedDescription
        }
    }

    func sendDraft() async {
        let text = draft.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !text.isEmpty, let endpoint else {
            return
        }
        state = .sending
        do {
            var request = URLRequest(url: endpoint.url(path: "/api/message"))
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.httpBody = try JSONEncoder().encode(MessagePost(message: text))
            let (_, response) = try await URLSession.shared.data(for: request)
            try validate(response: response)
            draft = ""
            state = .connected
        } catch {
            state = .offline
            errorMessage = error.localizedDescription
        }
    }

    func createFork() async {
        guard let endpoint else {
            return
        }
        state = .forking
        do {
            var request = URLRequest(url: endpoint.url(path: "/api/fork"))
            request.httpMethod = "POST"
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            let (data, response) = try await URLSession.shared.data(for: request)
            try validate(response: response)
            let payload = try JSONDecoder().decode(ForkResponse.self, from: data)
            forkCommand = payload.command
            state = .connected
        } catch {
            state = .offline
            errorMessage = error.localizedDescription
        }
    }

    private var endpoint: RemoteEndpoint? {
        guard let baseURL, let token else {
            return nil
        }
        return RemoteEndpoint(baseURL: baseURL, token: token)
    }

    private func fetchSnapshot(endpoint: RemoteEndpoint) async throws -> RemoteSnapshot {
        let (data, response) = try await URLSession.shared.data(from: endpoint.url(path: "/api/state"))
        try validate(response: response)
        return try JSONDecoder().decode(RemoteSnapshot.self, from: data)
    }

    private func startEventStream(endpoint: RemoteEndpoint) {
        eventsTask = Task { [weak self] in
            do {
                let (bytes, response) = try await URLSession.shared.bytes(from: endpoint.url(path: "/api/events"))
                try await MainActor.run {
                    try self?.validate(response: response)
                }
                var eventName = ""
                var dataLines: [String] = []

                for try await line in bytes.lines {
                    if Task.isCancelled {
                        return
                    }
                    if line.isEmpty {
                        if eventName == "snapshot", !dataLines.isEmpty {
                            let payload = dataLines.joined(separator: "\n")
                            let data = Data(payload.utf8)
                            let snapshot = try JSONDecoder().decode(RemoteSnapshot.self, from: data)
                            await MainActor.run {
                                self?.snapshot = snapshot
                                self?.state = .connected
                            }
                        }
                        eventName = ""
                        dataLines.removeAll(keepingCapacity: true)
                    } else if let event = line.strip(prefix: "event:") {
                        eventName = event.trimmingCharacters(in: .whitespaces)
                    } else if let data = line.strip(prefix: "data:") {
                        dataLines.append(data.trimmingCharacters(in: .whitespaces))
                    }
                }
            } catch {
                await MainActor.run {
                    guard self?.eventsTask?.isCancelled == false else {
                        return
                    }
                    self?.state = .reconnecting
                    self?.errorMessage = error.localizedDescription
                }
            }
        }
    }

    private func validate(response: URLResponse) throws {
        guard let http = response as? HTTPURLResponse else {
            throw RemoteError.invalidResponse
        }
        guard (200..<300).contains(http.statusCode) else {
            throw RemoteError.httpStatus(http.statusCode)
        }
    }
}

struct RemoteSnapshot: Decodable, Equatable {
    var cwd: String
    var status: String
    var messages: [RemoteMessage]
    var fork: ForkStatus?

    static let empty = RemoteSnapshot(
        cwd: "",
        status: "Ready",
        messages: [],
        fork: nil
    )
}

struct RemoteMessage: Decodable, Equatable, Identifiable {
    var id: Int
    var role: RemoteRole
    var text: String
}

enum RemoteRole: String, Decodable, Equatable {
    case user
    case assistant
    case tool
    case status
}

struct ForkStatus: Decodable, Equatable {
    var available: Bool
    var threadId: String?
}

private struct MessagePost: Encodable {
    var message: String
}

private struct ForkResponse: Decodable {
    var command: String
}

private struct RemoteEndpoint: Equatable {
    var baseURL: URL
    var token: String

    init(rawPairURL: String) throws {
        let trimmed = rawPairURL.trimmingCharacters(in: .whitespacesAndNewlines)
        let normalized = trimmed.contains("://") ? trimmed : "http://\(trimmed)"
        guard var components = URLComponents(string: normalized),
              let scheme = components.scheme,
              let host = components.host
        else {
            throw RemoteError.invalidPairURL
        }
        guard scheme == "http" else {
            throw RemoteError.unsupportedScheme
        }
        let token = components.queryItems?.first(where: { $0.name == "token" })?.value
        guard let token, !token.isEmpty else {
            throw RemoteError.missingToken
        }
        components.path = ""
        components.query = nil
        components.fragment = nil
        guard let baseURL = components.url else {
            throw RemoteError.invalidPairURL
        }
        self.baseURL = baseURL
        self.token = token

        if host.isEmpty {
            throw RemoteError.invalidPairURL
        }
    }

    init(baseURL: URL, token: String) {
        self.baseURL = baseURL
        self.token = token
    }

    func url(path: String) -> URL {
        var components = URLComponents()
        components.scheme = baseURL.scheme
        components.host = baseURL.host
        components.port = baseURL.port
        components.path = path
        components.queryItems = [URLQueryItem(name: "token", value: token)]
        return components.url!
    }
}

private enum RemoteError: LocalizedError {
    case invalidPairURL
    case unsupportedScheme
    case missingToken
    case invalidResponse
    case httpStatus(Int)

    var errorDescription: String? {
        switch self {
        case .invalidPairURL:
            "Invalid pair URL"
        case .unsupportedScheme:
            "Only local http:// pair URLs are supported"
        case .missingToken:
            "Pair URL is missing its token"
        case .invalidResponse:
            "Invalid response from Codex"
        case .httpStatus(let status):
            "Codex returned HTTP \(status)"
        }
    }
}

private extension String {
    func strip(prefix: String) -> String? {
        guard hasPrefix(prefix) else {
            return nil
        }
        return String(dropFirst(prefix.count))
    }
}
