import SwiftUI
import UIKit

struct ContentView: View {
    @ObservedObject var session: RemoteSession

    var body: some View {
        VStack(spacing: 0) {
            HeaderView(session: session)
            Divider()
                .overlay(Color.latteLine)
            if !session.isPaired {
                PairingView(session: session)
            } else {
                TranscriptView(snapshot: session.snapshot, forkCommand: session.forkCommand)
            }
            ComposerView(session: session)
        }
        .background(Color.latteBackground)
    }
}

private struct HeaderView: View {
    @ObservedObject var session: RemoteSession

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 12) {
                Text("Codex")
                    .font(.system(size: 22, weight: .semibold, design: .rounded))
                    .foregroundStyle(Color.latteInk)
                Spacer()
                Button {
                    Task {
                        await session.createFork()
                    }
                } label: {
                    Label("Fork", systemImage: "arrow.triangle.branch")
                        .labelStyle(.titleAndIcon)
                }
                .buttonStyle(SecondaryButtonStyle())
                .disabled(!session.canFork || session.state == .forking)
                .opacity(session.canFork ? 1 : 0.45)
                .accessibilityIdentifier("forkButton")

                Text(session.state.title)
                    .font(.system(size: 12, weight: .semibold))
                    .padding(.horizontal, 10)
                    .padding(.vertical, 7)
                    .foregroundStyle(session.state == .offline ? Color.redwood : Color.moss)
                    .background(
                        Capsule()
                            .fill(session.state == .offline ? Color.redwood.opacity(0.12) : Color.moss.opacity(0.14))
                    )
                    .accessibilityIdentifier("connectionStatus")
            }

            if !session.snapshot.cwd.isEmpty {
                Text(session.snapshot.cwd)
                    .font(.system(size: 12, design: .monospaced))
                    .lineLimit(1)
                    .truncationMode(.middle)
                    .foregroundStyle(Color.latteMuted)
                    .accessibilityIdentifier("cwdLabel")
            }

            if let error = session.errorMessage, session.state == .offline || session.state == .reconnecting {
                Text(error)
                    .font(.footnote)
                    .foregroundStyle(Color.redwood)
                    .lineLimit(2)
                    .accessibilityIdentifier("errorLabel")
            }
        }
        .padding(.horizontal, 18)
        .padding(.top, 16)
        .padding(.bottom, 12)
        .background(.ultraThinMaterial)
    }
}

private struct PairingView: View {
    @ObservedObject var session: RemoteSession

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Pair URL")
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(Color.latteMuted)
            TextField("http://192.168.1.20:49152?token=...", text: $session.pairURL)
                .textInputAutocapitalization(.never)
                .keyboardType(.URL)
                .autocorrectionDisabled()
                .font(.system(size: 15, design: .monospaced))
                .padding(12)
                .background(Color.lattePanel)
                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                        .stroke(Color.latteLine)
                )
                .accessibilityIdentifier("pairURLField")
            Button {
                Task {
                    await session.connect()
                }
            } label: {
                Label("Connect", systemImage: "link")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(PrimaryButtonStyle())
            .accessibilityIdentifier("connectButton")
        }
        .padding(18)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}

private struct TranscriptView: View {
    var snapshot: RemoteSnapshot
    var forkCommand: String?

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 12) {
                    if let forkCommand {
                        ForkCommandView(command: forkCommand)
                            .id("forkCommand")
                    }
                    if snapshot.messages.isEmpty {
                        EmptyTranscriptView()
                    } else {
                        ForEach(snapshot.messages) { message in
                            MessageRow(message: message)
                                .id(message.id)
                        }
                    }
                }
                .padding(.horizontal, 14)
                .padding(.vertical, 16)
            }
            .accessibilityIdentifier("transcriptScroll")
            .onChange(of: snapshot.messages.last?.id) { _, id in
                guard let id else {
                    return
                }
                withAnimation(.snappy(duration: 0.22)) {
                    proxy.scrollTo(id, anchor: .bottom)
                }
            }
            .onChange(of: forkCommand) { _, command in
                guard command != nil else {
                    return
                }
                withAnimation(.snappy(duration: 0.22)) {
                    proxy.scrollTo("forkCommand", anchor: .top)
                }
            }
        }
    }
}

private struct EmptyTranscriptView: View {
    var body: some View {
        Text("No transcript yet.")
            .font(.callout)
            .foregroundStyle(Color.latteMuted)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(16)
            .background(Color.lattePanel.opacity(0.72))
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .stroke(Color.latteLine, style: StrokeStyle(lineWidth: 1, dash: [5, 4]))
            )
    }
}

private struct ForkCommandView: View {
    var command: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Label("Fork ready", systemImage: "terminal")
                    .font(.system(size: 13, weight: .semibold))
                    .foregroundStyle(Color.moss)
                Spacer()
                Button {
                    UIPasteboard.general.string = command
                } label: {
                    Image(systemName: "doc.on.doc")
                        .font(.system(size: 14, weight: .semibold))
                }
                .buttonStyle(IconButtonStyle())
                .accessibilityLabel("Copy fork command")
            }
            Text(command)
                .font(.system(size: 12, design: .monospaced))
                .foregroundStyle(Color.latteInk)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(12)
        .background(Color.lattePanel)
        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .stroke(Color.moss.opacity(0.32))
        )
        .accessibilityIdentifier("forkCommand")
    }
}

private struct MessageRow: View {
    var message: RemoteMessage

    var body: some View {
        HStack {
            if message.role == .user {
                Spacer(minLength: 36)
            }
            Text(message.text)
                .font(message.role.isMachine ? .system(size: 12, design: .monospaced) : .body)
                .foregroundStyle(message.role.foreground)
                .textSelection(.enabled)
                .padding(.horizontal, 12)
                .padding(.vertical, 10)
                .background(message.role.background)
                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                .overlay(
                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                        .stroke(message.role.border)
                )
                .frame(maxWidth: 680, alignment: message.role == .user ? .trailing : .leading)
            if message.role != .user {
                Spacer(minLength: 36)
            }
        }
        .accessibilityIdentifier("message-\(message.id)")
    }
}

private struct ComposerView: View {
    @ObservedObject var session: RemoteSession

    var body: some View {
        VStack(spacing: 10) {
            Divider()
                .overlay(Color.latteLine)
            HStack(alignment: .bottom, spacing: 10) {
                TextField("Message Codex", text: $session.draft, axis: .vertical)
                    .lineLimit(1...5)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 11)
                    .background(Color.lattePanel)
                    .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                    .overlay(
                        RoundedRectangle(cornerRadius: 8, style: .continuous)
                            .stroke(Color.latteLine)
                    )
                    .accessibilityIdentifier("messageField")
                Button {
                    Task {
                        await session.sendDraft()
                    }
                } label: {
                    Image(systemName: "paperplane.fill")
                        .font(.system(size: 16, weight: .semibold))
                        .frame(width: 44, height: 44)
                }
                .buttonStyle(PrimaryIconButtonStyle())
                .disabled(!session.canSend || session.state == .sending)
                .accessibilityLabel("Send message")
            }
            .padding(.horizontal, 14)
            .padding(.bottom, 10)
        }
        .background(.ultraThinMaterial)
    }
}

private struct PrimaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.system(size: 15, weight: .semibold))
            .foregroundStyle(Color.lattePanel)
            .padding(.vertical, 12)
            .background(configuration.isPressed ? Color.moss.opacity(0.82) : Color.moss)
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
    }
}

private struct SecondaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.system(size: 13, weight: .semibold))
            .foregroundStyle(Color.moss)
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background(configuration.isPressed ? Color.moss.opacity(0.18) : Color.moss.opacity(0.12))
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
    }
}

private struct PrimaryIconButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .foregroundStyle(Color.lattePanel)
            .background(configuration.isPressed ? Color.moss.opacity(0.82) : Color.moss)
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
    }
}

private struct IconButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .foregroundStyle(Color.moss)
            .frame(width: 32, height: 32)
            .background(configuration.isPressed ? Color.moss.opacity(0.18) : Color.moss.opacity(0.1))
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
    }
}

private extension RemoteRole {
    var background: Color {
        switch self {
        case .user: Color.userBubble
        case .assistant: Color.assistantBubble
        case .tool, .status: Color.toolBubble
        }
    }

    var border: Color {
        switch self {
        case .user: Color.userBorder
        case .assistant: Color.latteLine
        case .tool, .status: Color.latteLine.opacity(0.8)
        }
    }

    var foreground: Color {
        switch self {
        case .user, .assistant: Color.latteInk
        case .tool, .status: Color.latteMuted
        }
    }

    var isMachine: Bool {
        self == .tool || self == .status
    }
}

private extension Color {
    static let latteBackground = Color(red: 0.965, green: 0.925, blue: 0.855)
    static let lattePanel = Color(red: 1.0, green: 0.982, blue: 0.94)
    static let latteInk = Color(red: 0.16, green: 0.135, blue: 0.105)
    static let latteMuted = Color(red: 0.47, green: 0.40, blue: 0.32)
    static let latteLine = Color(red: 0.82, green: 0.74, blue: 0.62)
    static let moss = Color(red: 0.18, green: 0.43, blue: 0.38)
    static let redwood = Color(red: 0.64, green: 0.21, blue: 0.16)
    static let userBubble = Color(red: 0.92, green: 0.84, blue: 0.73)
    static let userBorder = Color(red: 0.78, green: 0.66, blue: 0.52)
    static let assistantBubble = Color(red: 1.0, green: 0.965, blue: 0.91)
    static let toolBubble = Color(red: 0.91, green: 0.86, blue: 0.78)
}

#Preview {
    ContentView(session: RemoteSession())
}
