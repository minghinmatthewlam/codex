use qrcode::QrCode;
use qrcode::types::Color as QrColor;
use ratatui::style::Color;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::text::Span;

const QUIET_ZONE_MODULES: isize = 2;

pub(crate) fn render_qr_lines(content: &str) -> Vec<Line<'static>> {
    let Ok(code) = QrCode::new(content.as_bytes()) else {
        return vec![Line::from(content.to_string())];
    };
    let size = code.width() as isize;
    let style = Style::default().fg(Color::Black).bg(Color::White);
    let mut lines = Vec::new();
    let mut y = -QUIET_ZONE_MODULES;
    while y < size + QUIET_ZONE_MODULES {
        let mut row = String::new();
        for x in -QUIET_ZONE_MODULES..size + QUIET_ZONE_MODULES {
            row.push(
                match (module_is_dark(&code, x, y), module_is_dark(&code, x, y + 1)) {
                    (true, true) => '█',
                    (true, false) => '▀',
                    (false, true) => '▄',
                    (false, false) => ' ',
                },
            );
        }
        lines.push(Line::from(Span::styled(row, style)));
        y += 2;
    }
    lines
}

fn module_is_dark(code: &QrCode, x: isize, y: isize) -> bool {
    let size = code.width() as isize;
    if x < 0 || y < 0 || x >= size || y >= size {
        return false;
    }
    code[(x as usize, y as usize)] == QrColor::Dark
}
