package sync

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/0xPolygon/cdk/etherman"
	"github.com/0xPolygon/cdk/log"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	DefaultWaitPeriodBlockNotFound = time.Millisecond * 100
)

type EthClienter interface {
	ethereum.LogFilterer
	ethereum.BlockNumberReader
	ethereum.ChainReader
	bind.ContractBackend
}

type EVMDownloaderInterface interface {
	WaitForNewBlocks(ctx context.Context, lastBlockSeen uint64) (newLastBlock uint64)
	GetEventsByBlockRange(ctx context.Context, fromBlock, toBlock uint64) EVMBlocks
	GetLogs(ctx context.Context, fromBlock, toBlock uint64) []types.Log
	GetBlockHeader(ctx context.Context, blockNum uint64) (EVMBlockHeader, bool)
	GetLastFinalizedBlock(ctx context.Context) (*types.Header, error)
}

type LogAppenderMap map[common.Hash]func(b *EVMBlock, l types.Log) error

type EVMDownloader struct {
	syncBlockChunkSize uint64
	EVMDownloaderInterface
	log                *log.Logger
	finalizedBlockType etherman.BlockNumberFinality
}

func NewEVMDownloader(
	syncerID string,
	ethClient EthClienter,
	syncBlockChunkSize uint64,
	blockFinalityType etherman.BlockNumberFinality,
	waitForNewBlocksPeriod time.Duration,
	appender LogAppenderMap,
	adressessToQuery []common.Address,
	rh *RetryHandler,
	finalizedBlockType etherman.BlockNumberFinality,
) (*EVMDownloader, error) {
	logger := log.WithFields("syncer", syncerID)
	finality, err := blockFinalityType.ToBlockNum()
	if err != nil {
		return nil, err
	}

	topicsToQuery := make([]common.Hash, 0, len(appender))
	for topic := range appender {
		topicsToQuery = append(topicsToQuery, topic)
	}

	fbtEthermanType := finalizedBlockType
	fbt, err := finalizedBlockType.ToBlockNum()
	if err != nil {
		return nil, err
	}

	if fbt.Cmp(finality) > 0 {
		// if someone configured the syncer to query blocks by Safe or Finalized block
		// finalized block type should be at least the same as the block finality
		fbt = finality
		fbtEthermanType = blockFinalityType
		logger.Warnf("finalized block type %s is greater than block finality %s, setting finalized block type to %s",
			finalizedBlockType, blockFinalityType, fbtEthermanType)
	}

	logger.Infof("downloader initialized with block finality: %s, finalized block type: %s. SyncChunkSize: %d",
		blockFinalityType, fbtEthermanType, syncBlockChunkSize)

	return &EVMDownloader{
		syncBlockChunkSize: syncBlockChunkSize,
		log:                logger,
		finalizedBlockType: fbtEthermanType,
		EVMDownloaderInterface: &EVMDownloaderImplementation{
			ethClient:              ethClient,
			blockFinality:          finality,
			waitForNewBlocksPeriod: waitForNewBlocksPeriod,
			appender:               appender,
			topicsToQuery:          topicsToQuery,
			adressessToQuery:       adressessToQuery,
			rh:                     rh,
			log:                    logger,
			finalizedBlockType:     fbt,
		},
	}, nil
}

func (d *EVMDownloader) Download(ctx context.Context, fromBlock uint64, downloadedCh chan EVMBlock) {
	lastBlock := d.WaitForNewBlocks(ctx, 0)

	for {
		select {
		case <-ctx.Done():
			d.log.Info("closing evm downloader channel")
			close(downloadedCh)
			return
		default:
		}

		toBlock := fromBlock + d.syncBlockChunkSize
		if toBlock > lastBlock {
			toBlock = lastBlock
		}

		if fromBlock > toBlock {
			d.log.Infof(
				"waiting for new blocks, last block processed: %d, last block seen on L1: %d",
				fromBlock-1, lastBlock,
			)
			lastBlock = d.WaitForNewBlocks(ctx, fromBlock-1)
			continue
		}

		lastFinalizedBlock, err := d.GetLastFinalizedBlock(ctx)
		if err != nil {
			d.log.Error("error getting last finalized block: ", err)
			continue
		}

		lastFinalizedBlockNumber := lastFinalizedBlock.Number.Uint64()

		d.log.Infof("getting events from blocks %d to  %d. lastFinalizedBlock: %d",
			fromBlock, toBlock, lastFinalizedBlockNumber)
		blocks := d.GetEventsByBlockRange(ctx, fromBlock, toBlock)

		if toBlock <= lastFinalizedBlockNumber {
			// this is the case where all the blocks in range are finalized
			d.reportBlocks(downloadedCh, blocks, lastFinalizedBlockNumber)
			fromBlock = toBlock + 1

			if blocks.Len() == 0 || blocks[blocks.Len()-1].Num < toBlock {
				d.reportEmptyBlock(ctx, downloadedCh, toBlock, lastFinalizedBlockNumber)
			}
		} else {
			d.reportBlocks(downloadedCh, blocks, lastFinalizedBlockNumber)

			if blocks.Len() == 0 {
				if lastFinalizedBlockNumber > fromBlock &&
					lastFinalizedBlockNumber-fromBlock > d.syncBlockChunkSize {
					d.reportEmptyBlock(ctx, downloadedCh, fromBlock+d.syncBlockChunkSize, lastFinalizedBlockNumber)
					fromBlock += d.syncBlockChunkSize + 1
				}
			} else {
				fromBlock = blocks[blocks.Len()-1].Num + 1
			}

			// here we will wait for new blocks so we can expand the range
			// if we are already lagging behind the tip of the chain, the function will immediately return the latest block
			// if we are not lagging behind, but we are on tip of the chain, the function will wait until it has a new block
			// which will stop us from spamming the eth client with requests for the same block range
			// examples:
			// (sync chunk size = 10), no logs in the range:
			// 1. fromBlock = 1, toBlock = 10, lastFinalizedBlock = 15, lastBlock = 30
			//    - fromBlock = 16, toBlock = 30
			// 2. fromBlock = 10, toBlock = 20, lastFinalizedBlock = 5, lastBlock = 21
			//	  - fromBlock = 10, toBlock = 21
			// 3. fromBlock = 10, toBlock = 20, lastFinalizedBlock = 15, lastBlock = 21
			//    - fromBlock = 16, toBlock = 21
			// 4. fromBlock = 10, toBlock = 30, lastFinalizedBlock = 15, lastBlock = 50
			//   - fromBlock = 16, toBlock = 50
			// (sync chunk size = 10), logs in the range:
			// 1. fromBlock = 1, toBlock = 10, lastFinalizedBlock = 15, lastBlock = 30, lastBlockWithLogs = 8
			//    - fromBlock = 9, toBlock = 30
			// 2. fromBlock = 10, toBlock = 20, lastFinalizedBlock = 5, lastBlock = 21, lastBlockWithLogs = 15
			//	  - fromBlock = 16, toBlock = 21
			// 3. fromBlock = 10, toBlock = 20, lastFinalizedBlock = 15, lastBlock = 21, lastBlockWithLogs = 12
			//    - fromBlock = 13, toBlock = 21
			// 4. fromBlock = 10, toBlock = 50, lastFinalizedBlock = 15, lastBlock = 50, lastBlockWithLogs = 20
			//    - fromBlock = 21, toBlock = 50
			// IMPORTANT NOTE:
			// In this case, where we might have finalized blocks in range, or not, we will keep expanding the range
			// until we hit a log or we have finalzied blocks in range, where we will shorten the range, but,
			// keep in mind, that ethereum after the merge considers the block as finalzied after 2 epochs have passed,
			// or roughly 64 blocks, or ~12 minutes, so range will never be too big
			lastBlock = d.WaitForNewBlocks(ctx, toBlock)
			toBlock = lastBlock
		}
	}
}

func (d *EVMDownloader) reportBlocks(downloadedCh chan EVMBlock, blocks EVMBlocks, lastFinalizedBlock uint64) {
	for _, block := range blocks {
		d.log.Infof("sending block %d to the driver (with events)", block.Num)
		block.IsFinalizedBlock = d.finalizedBlockType.IsFinalized() && block.Num <= lastFinalizedBlock
		downloadedCh <- *block
	}
}

func (d *EVMDownloader) reportEmptyBlock(ctx context.Context, downloadedCh chan EVMBlock,
	blockNum, lastFinalizedBlock uint64) {
	// Indicate the last downloaded block if there are not events on it
	d.log.Debugf("sending block %d to the driver (without events)", blockNum)
	header, isCanceled := d.GetBlockHeader(ctx, blockNum)
	if isCanceled {
		return
	}

	downloadedCh <- EVMBlock{
		IsFinalizedBlock: d.finalizedBlockType.IsFinalized() && header.Num <= lastFinalizedBlock,
		EVMBlockHeader:   header,
	}
}

type EVMDownloaderImplementation struct {
	ethClient              EthClienter
	blockFinality          *big.Int
	waitForNewBlocksPeriod time.Duration
	appender               LogAppenderMap
	topicsToQuery          []common.Hash
	adressessToQuery       []common.Address
	rh                     *RetryHandler
	log                    *log.Logger
	finalizedBlockType     *big.Int
}

func NewEVMDownloaderImplementation(
	syncerID string,
	ethClient EthClienter,
	blockFinality *big.Int,
	waitForNewBlocksPeriod time.Duration,
	appender LogAppenderMap,
	topicsToQuery []common.Hash,
	adressessToQuery []common.Address,
	rh *RetryHandler,
) *EVMDownloaderImplementation {
	logger := log.WithFields("syncer", syncerID)
	return &EVMDownloaderImplementation{
		ethClient:              ethClient,
		blockFinality:          blockFinality,
		waitForNewBlocksPeriod: waitForNewBlocksPeriod,
		appender:               appender,
		topicsToQuery:          topicsToQuery,
		adressessToQuery:       adressessToQuery,
		rh:                     rh,
		log:                    logger,
	}
}

func (d *EVMDownloaderImplementation) GetLastFinalizedBlock(ctx context.Context) (*types.Header, error) {
	return d.ethClient.HeaderByNumber(ctx, d.finalizedBlockType)
}

func (d *EVMDownloaderImplementation) WaitForNewBlocks(
	ctx context.Context, lastBlockSeen uint64,
) (newLastBlock uint64) {
	attempts := 0
	ticker := time.NewTicker(d.waitForNewBlocksPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			d.log.Info("context cancelled")
			return lastBlockSeen
		case <-ticker.C:
			header, err := d.ethClient.HeaderByNumber(ctx, d.blockFinality)
			if err != nil {
				if ctx.Err() == nil {
					attempts++
					d.log.Error("error getting last block num from eth client: ", err)
					d.rh.Handle("waitForNewBlocks", attempts)
				} else {
					d.log.Warn("context has been canceled while trying to get header by number")
				}
				continue
			}
			if header.Number.Uint64() > lastBlockSeen {
				return header.Number.Uint64()
			}
		}
	}
}

func (d *EVMDownloaderImplementation) GetEventsByBlockRange(ctx context.Context, fromBlock, toBlock uint64) EVMBlocks {
	select {
	case <-ctx.Done():
		return nil
	default:
		blocks := EVMBlocks{}
		logs := d.GetLogs(ctx, fromBlock, toBlock)
		for _, l := range logs {
			if len(blocks) == 0 || blocks[len(blocks)-1].Num < l.BlockNumber {
				b, canceled := d.GetBlockHeader(ctx, l.BlockNumber)
				if canceled {
					return nil
				}

				if b.Hash != l.BlockHash {
					d.log.Infof(
						"there has been a block hash change between the event query and the block query "+
							"for block %d: %s vs %s. Retrying.",
						l.BlockNumber, b.Hash, l.BlockHash,
					)
					return d.GetEventsByBlockRange(ctx, fromBlock, toBlock)
				}
				blocks = append(blocks, &EVMBlock{
					EVMBlockHeader: EVMBlockHeader{
						Num:        l.BlockNumber,
						Hash:       l.BlockHash,
						Timestamp:  b.Timestamp,
						ParentHash: b.ParentHash,
					},
					Events: []interface{}{},
				})
			}

			for {
				attempts := 0
				err := d.appender[l.Topics[0]](blocks[len(blocks)-1], l)
				if err != nil {
					attempts++
					d.log.Error("error trying to append log: ", err)
					d.rh.Handle("getLogs", attempts)
					continue
				}
				break
			}
		}

		return blocks
	}
}

func filterQueryToString(query ethereum.FilterQuery) string {
	return fmt.Sprintf("FromBlock: %s, ToBlock: %s, Addresses: %s, Topics: %s",
		query.FromBlock.String(), query.ToBlock.String(), query.Addresses, query.Topics)
}

func (d *EVMDownloaderImplementation) GetLogs(ctx context.Context, fromBlock, toBlock uint64) []types.Log {
	query := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(fromBlock),
		Addresses: d.adressessToQuery,
		ToBlock:   new(big.Int).SetUint64(toBlock),
	}
	var (
		attempts       = 0
		unfilteredLogs []types.Log
		err            error
	)
	for {
		unfilteredLogs, err = d.ethClient.FilterLogs(ctx, query)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// context is canceled, we don't want to fatal on max attempts in this case
				return nil
			}

			attempts++
			d.log.Errorf("error calling FilterLogs to eth client: filter: %s err:%w ",
				filterQueryToString(query),
				err,
			)
			d.rh.Handle("getLogs", attempts)
			continue
		}
		break
	}
	logs := make([]types.Log, 0, len(unfilteredLogs))
	for _, l := range unfilteredLogs {
		for _, topic := range d.topicsToQuery {
			if l.Topics[0] == topic {
				logs = append(logs, l)
				break
			}
		}
	}
	return logs
}

func (d *EVMDownloaderImplementation) GetBlockHeader(ctx context.Context, blockNum uint64) (EVMBlockHeader, bool) {
	attempts := 0
	for {
		header, err := d.ethClient.HeaderByNumber(ctx, new(big.Int).SetUint64(blockNum))
		if err != nil {
			if errors.Is(err, context.Canceled) {
				// context is canceled, we don't want to fatal on max attempts in this case
				return EVMBlockHeader{}, true
			}
			if errors.Is(err, ethereum.NotFound) {
				// block num can temporary disappear from the execution client due to a reorg,
				// in this case, we want to wait and not panic
				log.Warnf("block %d not found on the ethereum client: %v", blockNum, err)
				if d.rh.RetryAfterErrorPeriod != 0 {
					time.Sleep(d.rh.RetryAfterErrorPeriod)
				} else {
					time.Sleep(DefaultWaitPeriodBlockNotFound)
				}
				continue
			}

			attempts++
			d.log.Errorf("error getting block header for block %d, err: %v", blockNum, err)
			d.rh.Handle("getBlockHeader", attempts)
			continue
		}
		return EVMBlockHeader{
			Num:        header.Number.Uint64(),
			Hash:       header.Hash(),
			ParentHash: header.ParentHash,
			Timestamp:  header.Time,
		}, false
	}
}
