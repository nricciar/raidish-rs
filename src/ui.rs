use crate::disk::BlockDevice;
use crate::fs::FileSystem;
use crate::inspect::BlockKind;

/// Terminal colors and symbols of the block kinds.
fn color(k: BlockKind) -> &'static str {
    match k {
        BlockKind::Superblock => "\x1b[48;5;196m", // Bright red background
        BlockKind::Uberblock => "\x1b[48;5;208m",  // Orange background
        BlockKind::SpaceMap => "\x1b[48;5;220m",   // Gold background
        BlockKind::ObjectTable => "\x1b[48;5;39m", // Bright blue background
        BlockKind::Directory => "\x1b[48;5;51m",   // Cyan background
        BlockKind::Data => "\x1b[48;5;34m",        // Green background
        BlockKind::Other => "\x1b[48;5;65m",       // Dull green background
        BlockKind::Free => "\x1b[48;5;240m",       // Dark gray background
        BlockKind::Slack => "\x1b[48;5;236m",      // Darker gray background
    }
}

fn symbol(k: BlockKind) -> &'static str {
    match k {
        BlockKind::Superblock => "S",
        BlockKind::Uberblock => "U",
        BlockKind::SpaceMap => "M",
        BlockKind::ObjectTable => "I",
        BlockKind::Directory => "N",
        BlockKind::Data => "D",
        BlockKind::Other => "o",
        BlockKind::Free => "·",
        BlockKind::Slack => " ",
    }
}

impl<D: BlockDevice> FileSystem<D> {
    /// Get terminal dimensions, defaulting to 80x24 if detection fails
    fn get_terminal_dimensions() -> (usize, usize) {
        if let Some((w, h)) = term_size::dimensions() {
            (w, h)
        } else {
            (80, 24)
        }
    }

    /// Display a visual map of block usage (now async)
    pub async fn display_block_map(&self) {
        let superblock = self.superblock().await.unwrap();

        const RESET: &str = "\x1b[0m";

        let total_blocks = superblock.total_blocks;
        let (term_width, term_height) = Self::get_terminal_dimensions();

        // Reserve space for labels and margins
        let available_width = term_width.saturating_sub(14);

        // Reserve space for headers, legend, statistics, and footer
        let available_height = term_height.saturating_sub(20).max(10);

        // Calculate total available characters in the grid
        let total_chars_available = available_width * available_height;

        // Calculate how many blocks each character should represent
        let blocks_per_char =
            ((total_blocks as f64) / (total_chars_available as f64)).ceil() as u64;
        let blocks_per_char = blocks_per_char.max(1);

        println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
        println!("║                          RAIDZ BLOCK USAGE MAP                             ║");
        println!("╚═══════════════════════════════════════════════════════════════════════════╝");
        println!();
        println!(
            "Total Blocks: {}  |  Blocks per char: {}  |  Grid: {}x{} ({}x{} terminal)",
            total_blocks,
            blocks_per_char,
            available_width,
            available_height,
            term_width,
            term_height
        );
        println!();

        let (layout, _) = self.layout().await;
        let totals = layout.totals();

        // Print legend
        println!("Legend:");
        for (i, kind) in BlockKind::ALL.iter().enumerate() {
            let count = totals[i];
            let percentage = (count as f64 / total_blocks as f64) * 100.0;
            println!(
                "  {}{} {}{} - {:>6} blocks ({:>5.1}%)",
                color(*kind),
                symbol(*kind),
                RESET,
                kind.name(),
                count,
                percentage
            );
        }
        println!();

        // Print the map: each character is the kind most of its blocks are.
        println!("Block Map:");
        println!();
        let row_blocks = blocks_per_char * available_width as u64;
        let mut row_start = 0u64;
        while row_start < total_blocks {
            print!("{:>10} │ ", row_start);
            for b in layout.buckets(row_start, row_blocks, available_width) {
                let k = b.dominant();
                print!("{}{}{}", color(k), symbol(k), RESET);
            }
            println!();
            row_start += row_blocks;
        }

        println!();

        // Print orphaned block warning if any exist
        if let Ok(orphaned) = self.find_orphaned_blocks().await {
            if !orphaned.is_empty() {
                let total_orphaned: u64 = orphaned.iter().map(|(_, len)| len).sum();
                println!(
                    "⚠️  WARNING: {} orphaned blocks detected in {} extents",
                    total_orphaned,
                    orphaned.len()
                );
                if orphaned.len() <= 10 {
                    println!("   Orphaned extents:");
                    for (start, len) in &orphaned {
                        println!("     - Block {} (length {})", start, len);
                    }
                }
            }
        }

        println!();
    }

    /// Display detailed metaslab information
    pub async fn display_metaslab_info(&self) {
        println!("\n╔═══════════════════════════════════════════════════════════════════════════╗");
        println!("║                         METASLAB INFORMATION                               ║");
        println!("╚═══════════════════════════════════════════════════════════════════════════╝\n");

        for (i, ms) in self.metaslabs.iter().enumerate() {
            let ms = ms.read().await;
            let total_blocks = ms.block_count;
            let free_blocks = ms.free_blocks();
            let deferred_blocks = ms.pending_free_blocks();
            let allocated_blocks = total_blocks - free_blocks - deferred_blocks;
            let utilization = (allocated_blocks as f64 / total_blocks as f64) * 100.0;
            let fragmentation = ms.free_extents.len();
            if allocated_blocks == 0 && deferred_blocks == 0 {
                continue;
            }

            println!("Metaslab #{}", i);
            println!(
                "  Range: {} - {}",
                ms.start_block,
                ms.start_block + ms.block_count - 1
            );
            println!("  Total: {} blocks", total_blocks);
            println!("  Used:  {} blocks ({:.1}%)", allocated_blocks, utilization);
            println!(
                "  Free:  {} blocks in {} extents ({} deferred)",
                free_blocks, fragmentation, deferred_blocks
            );
            match ms.active_slot {
                Some(slot) => println!(
                    "  Space map: slot {} synced at TXG {}{}",
                    slot,
                    ms.synced_txg,
                    if ms.dirty { " (dirty)" } else { "" }
                ),
                None => println!("  Space map: never synced"),
            }

            // Draw a simple bar chart
            let bar_width = 50;
            let filled = ((utilization / 100.0) * bar_width as f64) as usize;
            print!("  [");
            for j in 0..bar_width {
                if j < filled {
                    print!("█");
                } else {
                    print!("░");
                }
            }
            println!("]");
            println!();
        }
    }
}
