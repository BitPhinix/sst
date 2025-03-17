package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"

	"github.com/cloudflare/cloudflare-go"
	cloudflare4 "github.com/cloudflare/cloudflare-go/v4"
	"github.com/cloudflare/cloudflare-go/v4/option"
	"github.com/cloudflare/cloudflare-go/v4/workers"
	"github.com/gorilla/websocket"
	"github.com/sst/sst/v3/cmd/sst/mosaic/watcher"
	"github.com/sst/sst/v3/internal/util"
	"github.com/sst/sst/v3/pkg/bus"
	"github.com/sst/sst/v3/pkg/project"
	"github.com/sst/sst/v3/pkg/project/provider"
	"github.com/sst/sst/v3/pkg/runtime"
	"github.com/sst/sst/v3/pkg/runtime/worker"
)

type WorkerBuildEvent struct {
	WorkerID string
	Errors   []string
}

type WorkerUpdatedEvent struct {
	WorkerID string
}

type WorkerInvokedEvent struct {
	WorkerID  string
	TailEvent *TailEvent
}

func Start(ctx context.Context, proj *project.Project, args map[string]interface{}) error {
	prov, ok := proj.Provider("cloudflare")
	if !ok {
		return util.NewReadableError(nil, "Cloudflare provider not found in project configuration")
	}
	api := prov.(*provider.CloudflareProvider).Api()
	evts := bus.Subscribe(&project.CompleteEvent{}, &watcher.FileChangedEvent{}, &runtime.BuildInput{})
	builds := map[string]*runtime.BuildOutput{}
	targets := map[string]*runtime.BuildInput{}
	type tailRef struct {
		ID         string
		ScriptName string
		Account    *cloudflare.ResourceContainer
	}
	tails := map[string]tailRef{}

exit:
	for {
		select {
		case <-ctx.Done():
			break exit
		case unknown := <-evts:
			switch evt := unknown.(type) {
			case *runtime.BuildInput:
				target := evt
				if target.Runtime != "worker" {
					continue
				}
				targets[target.FunctionID] = target
				var properties worker.Properties
				json.Unmarshal(target.Properties, &properties)
				account := cloudflare.AccountIdentifier(properties.AccountID)
				if _, ok := tails[target.FunctionID]; !ok {
					slog.Info("cloudflare tail creating", "functionID", target.FunctionID)
					tail, err := api.StartWorkersTail(ctx, account, properties.ScriptName)
					if err != nil {
						slog.Error("error creating tail", "error", err)
						continue
					}
					tails[target.FunctionID] = tailRef{
						ID:         tail.ID,
						ScriptName: properties.ScriptName,
						Account:    account,
					}
					conn, _, err := websocket.DefaultDialer.DialContext(ctx, tail.URL, http.Header{
						"Sec-WebSocket-Protocol": []string{"trace-v1"},
					})
					if err != nil {
						slog.Info("error dialing websocket", "error", err)
						continue
					}
					go func(functionID string) {
						defer delete(tails, functionID)
						for {
							msg := &TailEvent{}
							err := conn.ReadJSON(msg)
							if err != nil {
								slog.Info("error reading websocket", "error", err)
								return
							}
							bus.Publish(&WorkerInvokedEvent{
								WorkerID:  functionID,
								TailEvent: msg,
							})
						}
					}(target.FunctionID)
				}
				if _, ok := builds[target.FunctionID]; ok {
					continue
				}
				output, err := proj.Runtime.Build(ctx, target)
				if err != nil {
					continue
				}
				builds[target.FunctionID] = output
			case *watcher.FileChangedEvent:
				for workerID, target := range targets {
					if proj.Runtime.ShouldRebuild(target.Runtime, workerID, evt.Path) {
						output, err := proj.Runtime.Build(ctx, target)
						if err != nil {
							continue
						}
						bus.Publish(&WorkerBuildEvent{
							WorkerID: target.FunctionID,
							Errors:   output.Errors,
						})
						builds[target.FunctionID] = output
						var properties worker.Properties
						json.Unmarshal(target.Properties, &properties)

						content, err := os.ReadFile(filepath.Join(output.Out, output.Handler))
						if err != nil {
							slog.Info("error reading file", "error", err, "out", filepath.Join(output.Out, output.Handler))
							continue
						}
						slog.Info("updating worker script", "functionID", target.FunctionID)
						client := cloudflare4.NewClient()
						formData := &bytes.Buffer{}
						writer := multipart.NewWriter(formData)
						metadata := workers.WorkerMetadataParam{
							MainModule: cloudflare4.F("worker.js"),
						}
						metadataBytes, err := json.Marshal(metadata)
						if err != nil {
							slog.Info("error creating part", "error", err)
							continue
						}
						err = writer.WriteField("metadata", string(metadataBytes))
						if err != nil {
							slog.Info("error creating part", "error", err)
							continue
						}
						h := textproto.MIMEHeader{}
						h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="%s"; filename="%s"`, "script", "worker.js"))
						h.Set("Content-Type", "application/javascript+module")
						part, err := writer.CreatePart(h)
						if err != nil {
							slog.Info("error creating part", "error", err)
							continue
						}
						_, err = part.Write(content)
						if err != nil {
							log.Fatalf("Error writing content: %v", err)
						}
						writer.Close()
						params := workers.ScriptContentUpdateParams{
							AccountID:              cloudflare4.F(properties.AccountID),
							CfWorkerMainModulePart: cloudflare4.F("worker.js"),
						}
						result, err := client.Workers.Scripts.Content.Update(
							ctx,
							properties.ScriptName,
							params,
							option.WithRequestBody(writer.FormDataContentType(), formData.Bytes()),
						)
						t, _ := json.Marshal(result)
						slog.Info("result", "result", string(t))
						if err != nil {
							slog.Info("error updating worker script", "error", err)
							continue
						}
						slog.Info("done worker script", "functionID", target.FunctionID)
						bus.Publish(&WorkerUpdatedEvent{
							WorkerID: target.FunctionID,
						})

					}
				}
				break
			}
		}
	}

	for _, tail := range tails {
		api.DeleteWorkersTail(ctx, tail.Account, tail.ScriptName, tail.ID)
	}

	return nil
}
