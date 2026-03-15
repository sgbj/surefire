import { useState, useMemo } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogFooter, DialogTrigger } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Switch } from '@/components/ui/switch';
import { Field, FieldLabel } from '@/components/ui/field';
import { Collapsible, CollapsibleTrigger, CollapsibleContent } from '@/components/ui/collapsible';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { Play, ChevronsUpDown } from 'lucide-react';
import { toast } from 'sonner';
import type { JsonSchema, JsonSchemaProperty } from '@/lib/api';

interface TriggerDialogProps {
  jobName: string;
  argumentsSchema?: JsonSchema;
  isPending: boolean;
  onTrigger: (opts?: { args?: unknown; notBefore?: string; notAfter?: string; priority?: number; deduplicationId?: string }) => void;
}

export function TriggerDialog({ jobName, argumentsSchema, isPending, onTrigger }: TriggerDialogProps) {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState<string>(argumentsSchema ? 'form' : 'json');
  const [formValues, setFormValues] = useState<Record<string, unknown>>({});
  const [jsonText, setJsonText] = useState('');
  const [notBefore, setNotBefore] = useState('');
  const [notAfter, setNotAfter] = useState('');
  const [priority, setPriority] = useState('');
  const [deduplicationId, setDeduplicationId] = useState('');

  const properties = argumentsSchema?.properties ?? {};
  const requiredFields = useMemo(() => new Set(argumentsSchema?.required ?? []), [argumentsSchema]);

  const resetForm = () => {
    setFormValues({});
    setJsonText('');
    setNotBefore('');
    setNotAfter('');
    setPriority('');
    setDeduplicationId('');
  };

  const handleOpenChange = (value: boolean) => {
    setOpen(value);
    if (!value) resetForm();
  };

  const buildArgs = (): unknown | undefined => {
    if (tab === 'json') {
      if (!jsonText.trim()) return undefined;
      try {
        return JSON.parse(jsonText);
      } catch {
        toast.error('Invalid JSON');
        return null; // signal error
      }
    }

    // Form mode — build args object from form values
    if (Object.keys(properties).length === 0) return undefined;

    const args: Record<string, unknown> = {};
    let hasValues = false;

    for (const [name, prop] of Object.entries(properties)) {
      const value = formValues[name];
      if (value === undefined || value === '') continue;

      const resolvedType = resolveType(prop);
      if (resolvedType === 'integer' || resolvedType === 'number') {
        const num = Number(value);
        if (isNaN(num)) {
          toast.error(`"${name}" must be a number`);
          return null;
        }
        args[name] = resolvedType === 'integer' ? Math.trunc(num) : num;
      } else if (resolvedType === 'boolean') {
        args[name] = value;
      } else if (resolvedType === 'array' || resolvedType === 'object') {
        try {
          args[name] = JSON.parse(String(value));
        } catch {
          toast.error(`"${name}" must be valid JSON`);
          return null;
        }
      } else {
        args[name] = value;
      }
      hasValues = true;
    }

    return hasValues ? args : undefined;
  };

  const handleRun = () => {
    const args = buildArgs();
    if (args === null) return; // validation error

    const opts: { args?: unknown; notBefore?: string; notAfter?: string; priority?: number; deduplicationId?: string } = {};
    if (args !== undefined) opts.args = args;
    if (notBefore) opts.notBefore = new Date(notBefore).toISOString();
    if (notAfter) opts.notAfter = new Date(notAfter).toISOString();
    if (priority) {
      const p = parseInt(priority, 10);
      if (!isNaN(p)) opts.priority = p;
    }
    if (deduplicationId.trim()) opts.deduplicationId = deduplicationId.trim();

    onTrigger(Object.keys(opts).length > 0 ? opts : undefined);
    setOpen(false);
    resetForm();
  };

  const hasSchema = Object.keys(properties).length > 0;

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button variant="outline" className="cursor-pointer">
          <Play className="size-3.5" />
          Run
        </Button>
      </DialogTrigger>
      <DialogContent className="max-w-lg max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>Run {jobName}</DialogTitle>
          <DialogDescription className="sr-only">Configure arguments and options for this job run.</DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2 overflow-y-auto min-h-0 -mx-6 px-6">
          {/* Arguments section */}
          {hasSchema ? (
            <Tabs value={tab} onValueChange={setTab}>
              <TabsList className="w-full">
                <TabsTrigger value="form" className="flex-1">Form</TabsTrigger>
                <TabsTrigger value="json" className="flex-1">JSON</TabsTrigger>
              </TabsList>
              <TabsContent value="form" className="space-y-3 pt-3">
                {Object.entries(properties).map(([name, prop]) => (
                  <SchemaField
                    key={name}
                    name={name}
                    schema={prop}
                    required={requiredFields.has(name)}
                    value={formValues[name]}
                    onChange={(value) => setFormValues(prev => ({ ...prev, [name]: value }))}
                  />
                ))}
              </TabsContent>
              <TabsContent value="json" className="pt-3">
                <JsonEditor id="trigger-args-json" value={jsonText} onChange={setJsonText} />
              </TabsContent>
            </Tabs>
          ) : (
            <Field>
              <FieldLabel htmlFor="trigger-args">Arguments (JSON)</FieldLabel>
              <JsonEditor id="trigger-args" value={jsonText} onChange={setJsonText} />
            </Field>
          )}

          {/* Run Options (collapsible) */}
          <Collapsible>
            <CollapsibleTrigger className="flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors cursor-pointer">
              <ChevronsUpDown className="size-3.5" />
              Run options
            </CollapsibleTrigger>
            <CollapsibleContent className="space-y-3 pt-3">
              <Field>
                <FieldLabel htmlFor="trigger-not-before">Not before</FieldLabel>
                <Input
                  id="trigger-not-before"
                  type="datetime-local"
                  value={notBefore}
                  onChange={(e) => setNotBefore(e.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="trigger-not-after">Not after</FieldLabel>
                <Input
                  id="trigger-not-after"
                  type="datetime-local"
                  value={notAfter}
                  onChange={(e) => setNotAfter(e.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="trigger-priority">Priority</FieldLabel>
                <Input
                  id="trigger-priority"
                  type="number"
                  placeholder="0"
                  value={priority}
                  onChange={(e) => setPriority(e.target.value)}
                />
              </Field>
              <Field>
                <FieldLabel htmlFor="trigger-dedup">Deduplication ID</FieldLabel>
                <Input
                  id="trigger-dedup"
                  type="text"
                  placeholder="Optional"
                  value={deduplicationId}
                  onChange={(e) => setDeduplicationId(e.target.value)}
                />
              </Field>
            </CollapsibleContent>
          </Collapsible>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)}>Cancel</Button>
          <Button onClick={handleRun} disabled={isPending}>Run</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function JsonEditor({ id, value, onChange }: { id: string; value: string; onChange: (v: string) => void }) {
  return (
    <textarea
      id={id}
      className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[100px] outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]"
      placeholder='{"key": "value"}'
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}

function resolveType(prop: JsonSchemaProperty): string | undefined {
  if (prop.type) {
    if (Array.isArray(prop.type)) {
      // Filter out "null" to get the real type (e.g., ["integer", "null"] → "integer")
      const nonNull = prop.type.filter(t => t !== 'null');
      return nonNull[0];
    }
    return prop.type;
  }
  // Handle nullable types via oneOf/anyOf (e.g., { oneOf: [{ type: "integer" }, { type: "null" }] })
  const variants = prop.oneOf ?? prop.anyOf;
  if (variants) {
    const nonNull = variants.find(v => v.type !== 'null');
    if (nonNull?.type) return Array.isArray(nonNull.type) ? nonNull.type[0] : nonNull.type;
  }
  return undefined;
}

function isNullable(prop: JsonSchemaProperty): boolean {
  if (Array.isArray(prop.type) && prop.type.includes('null')) return true;
  const variants = prop.oneOf ?? prop.anyOf;
  if (variants?.some(v => v.type === 'null')) return true;
  return false;
}

interface SchemaFieldProps {
  name: string;
  schema: JsonSchemaProperty;
  required: boolean;
  value: unknown;
  onChange: (value: unknown) => void;
}

function SchemaField({ name, schema, required, value, onChange }: SchemaFieldProps) {
  const type = resolveType(schema);
  const nullable = isNullable(schema);
  const label = name + (required ? '' : nullable ? ' (optional)' : schema.default !== undefined ? ` (default: ${schema.default})` : ' (optional)');
  const fieldId = `field-${name}`;

  if (type === 'boolean') {
    return (
      <Field orientation="horizontal">
        <FieldLabel htmlFor={fieldId}>{label}</FieldLabel>
        <Switch
          id={fieldId}
          checked={value === true}
          onCheckedChange={(checked) => onChange(checked)}
        />
      </Field>
    );
  }

  if (schema.enum && schema.enum.length > 0) {
    return (
      <Field>
        <FieldLabel htmlFor={fieldId}>{label}</FieldLabel>
        <select
          id={fieldId}
          className="h-9 w-full rounded-md border bg-background px-3 py-1 text-sm outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]"
          value={String(value ?? '')}
          onChange={(e) => onChange(e.target.value || undefined)}
        >
          <option value="">—</option>
          {schema.enum.map((v) => (
            <option key={String(v)} value={String(v)}>{String(v)}</option>
          ))}
        </select>
      </Field>
    );
  }

  if (type === 'integer' || type === 'number') {
    return (
      <Field>
        <FieldLabel htmlFor={fieldId}>{label}</FieldLabel>
        <Input
          id={fieldId}
          type="number"
          step={type === 'integer' ? '1' : 'any'}
          min={schema.minimum}
          max={schema.maximum}
          placeholder={schema.default !== undefined ? String(schema.default) : undefined}
          value={String(value ?? '')}
          onChange={(e) => onChange(e.target.value || undefined)}
        />
      </Field>
    );
  }

  if (type === 'array' || type === 'object') {
    return (
      <Field>
        <FieldLabel htmlFor={fieldId}>{label} <span className="text-muted-foreground font-normal">({type})</span></FieldLabel>
        <textarea
          id={fieldId}
          className="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono min-h-[60px] outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]"
          placeholder={type === 'array' ? '["item1", "item2"]' : '{"key": "value"}'}
          value={String(value ?? '')}
          onChange={(e) => onChange(e.target.value || undefined)}
        />
      </Field>
    );
  }

  // Default: string input
  return (
    <Field>
      <FieldLabel htmlFor={fieldId}>{label}</FieldLabel>
      <Input
        id={fieldId}
        type="text"
        placeholder={schema.default !== undefined ? String(schema.default) : undefined}
        value={String(value ?? '')}
        onChange={(e) => onChange(e.target.value || undefined)}
      />
    </Field>
  );
}
