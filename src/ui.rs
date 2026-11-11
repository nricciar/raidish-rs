use crate::disk::BlockDevice;
use crate::fs::{FileSystem, SUPERBLOCK_LBA};
use crate::metaslab::SPACEMAP_SLOTS;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum BlockType {
    Superblock,
    Uberblock,
    SpaceMap,
    FileIndex,
    InodeData, // NEW: inode storage blocks
    FileData,
    Free,
}

impl BlockType {
    fn color(&self) -> &'static str {
        match self {
            BlockType::Superblock => "\x1b[48;5;196m", // Bright red background
            BlockType::Uberblock => "\x1b[48;5;208m",  // Orange background
            BlockType::SpaceMap => "\x1b[48;5;220m",   // Gold background
            BlockType::FileIndex => "\x1b[48;5;39m",   // Bright blue background
            BlockType::InodeData => "\x1b[48;5;51m",   // Cyan background
            BlockType::FileData => "\x1b[48;5;34m",    // Green background
            BlockType::Free => "\x1b[48;5;240m",       // Dark gray background
        }
    }

    fn symbol(&self) -> &'static str {
        match self {
            BlockType::Superblock => "S",
            BlockType::Uberblock => "U",
            BlockType::SpaceMap => "M",
            BlockType::FileIndex => "I",
            BlockType::InodeData => "N",
            BlockType::FileData => "D",
            BlockType::Free => "·",
        }
    }

    fn name(&self) -> &'static str {
        match self {
            BlockType::Superblock => "Superblock",
            BlockType::Uberblock => "Uberblock",
            BlockType::SpaceMap => "Space Map Slots",
            BlockType::FileIndex => "File Index",
            BlockType::InodeData => "Inode Data",
            BlockType::FileData => "File Data",
            BlockType::Free => "Free Space",
        }
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

    /// Build a complete block type map efficiently (async version)
    async fn build_block_map(&self) -> Vec<BlockType> {
        let superblock = self.superblock().await.unwrap();
        let total_blocks = superblock.total_blocks as usize;
        let mut block_map = vec![BlockType::FileData; total_blocks]; // Default to allocated

        // Mark free blocks first (since they're the base state for data region)
        for ms in self.metaslabs.iter() {
            let ms = ms.read().await;
            let pending = ms
                .freeing
                .iter()
                .chain(ms.deferred.iter().flat_map(|(_, v)| v.iter()))
                .map(|e| (e.start, e.len));
            for (free_start, free_len) in
                ms.free_extents.iter().map(|(&s, &l)| (s, l)).chain(pending)
            {
                let start = free_start as usize;
                let end = (free_start + free_len) as usize;
                for i in start..end.min(total_blocks) {
                    block_map[i] = BlockType::Free;
                }
            }
        }

        // Mark inode data blocks
        for inode_ref in self.file_index.read().await.files.values() {
            for extent in &inode_ref.inode_extents() {
                let start = extent.start() as usize;
                let end = (extent.start() + extent.len()) as usize;
                for i in start..end.min(total_blocks) {
                    block_map[i] = BlockType::InodeData;
                }
            }
        }

        // Mark file data (requires reading inodes)
        for inode_ref in self.file_index.read().await.files.values() {
            if let Ok(inode) = self.read_inode(inode_ref).await {
                for extent in &self
                    .content_extents(&inode.inode_type)
                    .await
                    .unwrap_or_default()
                {
                    let start = extent.start() as usize;
                    let end = (extent.start() + extent.len()) as usize;
                    for i in start..end.min(total_blocks) {
                        block_map[i] = BlockType::FileData;
                    }
                }
            }
        }

        // Mark file index
        for extent in self.root_ptr.lock().await.iter().map(|p| p.extent()) {
            let start = extent.start() as usize;
            let end = (extent.start() + extent.len()) as usize;
            for i in start..end.min(total_blocks) {
                block_map[i] = BlockType::FileIndex;
            }
        }

        // Mark space map slots (these override data allocations)
        let spacemap_start = superblock.spacemap_start as usize;
        let spacemap_end = spacemap_start
            + (superblock.metaslab_count * SPACEMAP_SLOTS * superblock.spacemap_slot_blocks)
                as usize;
        for i in spacemap_start..spacemap_end.min(total_blocks) {
            block_map[i] = BlockType::SpaceMap;
        }

        // Mark uberblocks
        let uberblock_start = self.header.uberblock_start as usize;
        let uberblock_end = (self.header.uberblock_start + self.header.uberblock_blocks()) as usize;
        for i in uberblock_start..uberblock_end.min(total_blocks) {
            block_map[i] = BlockType::Uberblock;
        }

        // Mark superblock
        for lba in SUPERBLOCK_LBA..SUPERBLOCK_LBA + self.header.superblock_blocks {
            if lba < total_blocks as u64 {
                block_map[lba as usize] = BlockType::Superblock;
            }
        }

        block_map
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

        // Build the complete block map once
        let block_map = self.build_block_map().await;

        // Count blocks by type for statistics
        let mut counts = std::collections::HashMap::new();
        for &block_type in &block_map {
            *counts.entry(block_type).or_insert(0u64) += 1;
        }

        // Print legend
        println!("Legend:");
        for block_type in [
            BlockType::Superblock,
            BlockType::Uberblock,
            BlockType::SpaceMap,
            BlockType::FileIndex,
            BlockType::InodeData,
            BlockType::FileData,
            BlockType::Free,
        ] {
            let count = counts.get(&block_type).copied().unwrap_or(0);
            let percentage = (count as f64 / total_blocks as f64) * 100.0;
            println!(
                "  {}{} {}{} - {:>6} blocks ({:>5.1}%)",
                block_type.color(),
                block_type.symbol(),
                RESET,
                block_type.name(),
                count,
                percentage
            );
        }
        println!();

        // Print the map
        println!("Block Map:");
        println!();

        let mut block_idx = 0u64;

        while block_idx < total_blocks {
            // Print block number label
            print!("{:>10} │ ", block_idx);

            for _col in 0..available_width {
                if block_idx >= total_blocks {
                    break;
                }

                // Sample blocks in this range to determine dominant type
                let end_block = (block_idx + blocks_per_char).min(total_blocks);
                let mut type_counts = std::collections::HashMap::new();

                for b in block_idx..end_block {
                    let bt = block_map[b as usize];
                    *type_counts.entry(bt).or_insert(0) += 1;
                }

                // Find the most common block type in this range
                let dominant_type = type_counts
                    .iter()
                    .max_by_key(|(_, count)| *count)
                    .map(|(bt, _)| *bt)
                    .unwrap_or(BlockType::Free);

                print!(
                    "{}{}{}",
                    dominant_type.color(),
                    dominant_type.symbol(),
                    RESET
                );

                block_idx += blocks_per_char;
            }

            println!();
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
