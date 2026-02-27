import { MarkdownRenderer } from "./MarkdownRenderer";

interface MarkdownEditorProps {
  value: string;
  onChange: (value: string) => void;
}

export function MarkdownEditor({ value, onChange }: MarkdownEditorProps) {
  return (
    <div className="grid grid-cols-2 gap-0 h-full min-h-0 border rounded-md overflow-hidden">
      <div className="flex flex-col min-h-0 border-r">
        <div className="px-3 py-2 border-b bg-muted/50 text-sm font-medium shrink-0">
          Markdown
        </div>
        <textarea
          className="flex-1 p-4 font-mono text-sm resize-none bg-background focus:outline-none"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          spellCheck={false}
        />
      </div>
      <div className="flex flex-col min-h-0">
        <div className="px-3 py-2 border-b bg-muted/50 text-sm font-medium shrink-0">
          Preview
        </div>
        <div className="flex-1 overflow-y-auto p-6">
          <MarkdownRenderer content={value} />
        </div>
      </div>
    </div>
  );
}
